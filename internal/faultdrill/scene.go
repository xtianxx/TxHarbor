//go:build fault

// scene.go is the T019 real-service scene of the five-state drills: it starts
// the actual `serve` process (with the 013 capacity guard assembly), the real
// `event-publisher` and `event-consumer` command entry points, the real
// PostgreSQL/Redis/Kafka containers and a local Anvil chain; it seeds the
// 007/011 prerequisites through the real public functions and exposes the
// HTTP/database probes the matrix tasks assert against.
//
// Discipline:
//
//   - every operation goes through the real service entry points (HTTP
//     handlers started by serve, scanner loops started by serve, the real
//     command loops) — no hand-called state function replaces a service path;
//   - the scene fails loudly: a process that exits early, an unexpected HTTP
//     status or a timeout is an error with diagnostics (serve stdout/stderr,
//     scanner logs, metric exposition);
//   - never fabricates an observation or a financial row: deposits come from
//     the local chain, receipts from POST /withdrawals.
package faultdrill

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/testutil"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// Scene constants (the same canonical fixture shape the E2E layer uses).
const (
	SceneChainID   = int64(31337)
	SceneAsset     = "0x1111111111111111111111111111111111111111"
	SceneWatch     = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	SceneRecipient = "0x3333333333333333333333333333333333333333"
	SceneCaller    = int64(13)
	SceneAuthID    = "authz-scene-1"
	SceneAmount    = "1000"
)

// syncBuffer is a concurrency-safe writer for subprocess/stdlib log capture.
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

// SceneOptions tunes the serve configuration of one drill scene.
type SceneOptions struct {
	// Capacity limits (raw env values). Zero values fall back to the
	// bench-scale defaults used by the matrix tasks.
	SoftLimit string
	HardLimit string
	Reserve   string
	// ConfirmationDepth defaults to "1".
	ConfirmationDepth string
	// StartRuntimes starts the real event-publisher and event-consumer loops.
	StartRuntimes bool
	// PublisherBatch overrides the publisher's claim batch (raw env value);
	// empty keeps the configured default.
	PublisherBatch string
}

// Scene is one running real-service environment.
type Scene struct {
	T   *testing.T
	Ctx context.Context

	Env    *Env
	Anvil  *Anvil
	Pool   *pgxpool.Pool
	Sender common.Address

	BaseURL string
	EnvMap  map[string]string

	APIKey string

	serveCancel context.CancelFunc
	serveDone   chan int
	serveOut    *syncBuffer
	serveErr    *syncBuffer
	logBuf      *syncBuffer
	prevSlog    *slog.Logger

	pubCancel  context.CancelFunc
	pubDone    chan int
	consCancel context.CancelFunc
	consDone   chan int
}

// StartScene boots dependencies, starts the real serve process and seeds the
// withdrawal prerequisites. The caller owns the scene via t.Cleanup (Stop is
// registered here).
func StartScene(t *testing.T, ctx context.Context, opts SceneOptions) *Scene {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	env, err := StartEnv(ctx)
	if err != nil {
		t.Fatalf("StartEnv: %v", err)
	}
	t.Cleanup(func() { _ = env.Close(context.Background()) })

	anvil, err := env.StartAnvil(ctx)
	if err != nil {
		t.Fatalf("StartAnvil: %v", err)
	}
	accounts, err := anvil.Accounts()
	if err != nil {
		t.Fatalf("anvil accounts: %v", err)
	}

	if opts.SoftLimit == "" {
		opts.SoftLimit = "100000"
	}
	if opts.HardLimit == "" {
		opts.HardLimit = "200000"
	}
	if opts.Reserve == "" {
		opts.Reserve = "1000"
	}
	if opts.ConfirmationDepth == "" {
		opts.ConfirmationDepth = "1"
	}

	scene := &Scene{
		T: t, Ctx: ctx, Env: env, Anvil: anvil, Pool: env.Pool, Sender: accounts[0],
		serveOut: &syncBuffer{}, serveErr: &syncBuffer{}, logBuf: &syncBuffer{},
	}
	scene.EnvMap = sceneEnv(env, anvil, opts)
	scene.BaseURL = "http://" + scene.EnvMap["TXHARBOR_HTTP_ADDR"]
	t.Cleanup(scene.Stop)

	// Capture the standard library logger: the 003/004 scanner pause evidence
	// is emitted through slog.Default().
	scene.prevSlog = slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(scene.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(scene.prevSlog) })

	// The real serve process: HTTP routes, scanners, dependency probes and the
	// 013 capacity guard assembly.
	scene.serveCancel, scene.serveDone = runCommand(ctx, func(runCtx context.Context) int {
		return app.Serve(runCtx, app.Deps{
			Getenv:  sceneGetenv(scene.EnvMap),
			Stdout:  scene.serveOut,
			Stderr:  scene.serveErr,
			Signals: make(chan os.Signal),
		})
	})
	scene.waitReady()

	// The deposit scanner bootstraps deposit_config_history from the serve
	// configuration; wait for it before any 007 create (the allowlist reader
	// consumes the newest version).
	if err := WaitFor(ctx, 90*time.Second, func() (bool, error) {
		var n int64
		if err := scene.Pool.QueryRow(ctx,
			`SELECT count(*) FROM deposit_config_history WHERE chain_id = $1`, SceneChainID).Scan(&n); err != nil {
			return false, err
		}
		return n > 0, nil
	}); err != nil {
		t.Fatalf("deposit config history bootstrap: %v%s", err, scene.diagnostics())
	}

	scene.seedPrerequisites()
	if opts.StartRuntimes {
		scene.StartRuntimes()
	}
	return scene
}

// sceneEnv builds the serve/command environment for one scene.
func sceneEnv(env *Env, anvil *Anvil, opts SceneOptions) map[string]string {
	httpAddr := freeLoopbackAddr()
	out := map[string]string{
		"TXHARBOR_PG_DSN":                  env.DSN,
		"TXHARBOR_RPC_URL":                 anvil.URL,
		"TXHARBOR_CHAIN_ID":                fmt.Sprintf("%d", SceneChainID),
		"TXHARBOR_START_HEIGHT":            "0",
		"TXHARBOR_LOG_START_HEIGHT":        "0",
		"TXHARBOR_LOG_CONTRACTS":           SceneAsset,
		"TXHARBOR_DEPOSIT_START_HEIGHT":    "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":       SceneAsset + ":0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES": SceneWatch + ":0",
		"TXHARBOR_CONFIRMATION_DEPTH":      opts.ConfirmationDepth,
		"TXHARBOR_REORG_MAX_DEPTH":         "100",
		"TXHARBOR_HTTP_ADDR":               httpAddr,
		"TXHARBOR_REDIS_ADDR":              env.Redis.HostPort(),
		"TXHARBOR_REDIS_TIMEOUT":           "1s",
		// 013 event infrastructure: the complete fail-closed configuration.
		"TXHARBOR_EVENTS_ENABLED":                      "true",
		"TXHARBOR_KAFKA_BROKERS":                       strings.Join(env.Kafka.Brokers(), ","),
		"TXHARBOR_KAFKA_TOPIC":                         testutil.KafkaTopic,
		"TXHARBOR_RATELIMIT_NEW_WITHDRAWAL":            "50/10",
		"TXHARBOR_RATELIMIT_WRITE":                     "50/10",
		"TXHARBOR_RATELIMIT_QUERY":                     "50/10",
		"TXHARBOR_RATELIMIT_OPERATOR":                  "50/10",
		"TXHARBOR_RATELIMIT_RPC":                       "50/10",
		"TXHARBOR_EVENTS_CAPACITY_SOFT_LIMIT":          opts.SoftLimit,
		"TXHARBOR_EVENTS_CAPACITY_HARD_LIMIT":          opts.HardLimit,
		"TXHARBOR_EVENTS_CAPACITY_RESERVE":             opts.Reserve,
		"TXHARBOR_EVENTS_CAPACITY_RETENTION":           "168h",
		"TXHARBOR_EVENTS_CAPACITY_MAX_SHUTDOWN_WINDOW": "1h",
		"TXHARBOR_EVENTS_CAPACITY_DRAIN_TARGET_WINDOW": "5m",
		"TXHARBOR_EVENTS_CONSUMER_GAP_WAIT":            "2s",
		"TXHARBOR_EVENTS_CONSUMER_POLL_INTERVAL":       "200ms",
		"TXHARBOR_EVENTS_PUBLISHER_POLL_INTERVAL":      "200ms",
	}
	if opts.PublisherBatch != "" {
		out["TXHARBOR_EVENTS_PUBLISHER_BATCH"] = opts.PublisherBatch
	}
	return out
}

// sceneGetenv adapts the scene map to the app.Deps shape.
func sceneGetenv(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

// freeLoopbackAddr reserves one loopback address.
func freeLoopbackAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("reserve loopback address: %v", err))
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// runCommand starts one command function in a goroutine.
func runCommand(ctx context.Context, run func(context.Context) int) (context.CancelFunc, chan int) {
	cmdCtx, cancel := context.WithCancel(ctx)
	done := make(chan int, 1)
	go func() { done <- run(cmdCtx) }()
	return cancel, done
}

// waitReady waits for /readyz 200 and fails with diagnostics otherwise.
func (s *Scene) waitReady() {
	s.T.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-s.serveDone:
			s.serveDone <- code
			s.T.Fatalf("serve exited early (code %d)%s", code, s.diagnostics())
		default:
		}
		resp, err := http.Get(s.BaseURL + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.T.Fatalf("serve never became ready%s", s.diagnostics())
}

// diagnostics renders captured output for failure messages.
func (s *Scene) diagnostics() string {
	return fmt.Sprintf("\nserve stdout:\n%s\nserve stderr:\n%s\nscanner logs:\n%s",
		s.serveOut.String(), s.serveErr.String(), s.logBuf.String())
}

// Stop shuts the runtimes and the serve process down cleanly.
func (s *Scene) Stop() {
	if s == nil {
		return
	}
	s.StopRuntimes()
	if s.serveCancel != nil {
		s.serveCancel()
		select {
		case code := <-s.serveDone:
			if code != 0 {
				s.T.Logf("serve exit code %d%s", code, s.diagnostics())
			}
		case <-time.After(30 * time.Second):
			s.T.Logf("serve did not stop within 30s%s", s.diagnostics())
		}
	}
}

// StartRuntimes starts the real event-publisher and event-consumer commands.
func (s *Scene) StartRuntimes() {
	s.T.Helper()
	if s.pubCancel != nil || s.consCancel != nil {
		return
	}
	deps := app.Deps{
		Getenv:  sceneGetenv(s.EnvMap),
		Stdout:  io.Discard,
		Stderr:  s.serveErr,
		Signals: make(chan os.Signal),
	}
	s.pubCancel, s.pubDone = runCommand(s.Ctx, func(runCtx context.Context) int {
		return app.EventPublisher(runCtx, nil, deps)
	})
	// The publisher refuses to start until the topic is reachable; the
	// consumer creates the reference-ledger schema before consuming.
	s.consCancel, s.consDone = runCommand(s.Ctx, func(runCtx context.Context) int {
		return app.EventConsumer(runCtx, nil, deps)
	})
	if err := WaitFor(s.Ctx, 90*time.Second, func() (bool, error) {
		if err := s.checkRuntimes(); err != nil {
			return false, err
		}
		var exists bool
		if err := s.Pool.QueryRow(s.Ctx,
			`SELECT to_regclass($1) IS NOT NULL`, events.RefLedgerTable).Scan(&exists); err != nil {
			return false, err
		}
		return exists, nil
	}); err != nil {
		s.T.Fatalf("runtimes did not start: %v%s", err, s.diagnostics())
	}
}

// checkRuntimes fails when a started runtime exited early.
func (s *Scene) checkRuntimes() error {
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
	if err := check("event-publisher", s.pubDone); err != nil {
		return err
	}
	return check("event-consumer", s.consDone)
}

// StopRuntimes stops the publisher and consumer loops cleanly.
func (s *Scene) StopRuntimes() {
	if s.pubCancel != nil {
		s.pubCancel()
		select {
		case code := <-s.pubDone:
			if code != 0 {
				s.T.Logf("event-publisher exit code %d%s", code, s.diagnostics())
			}
		case <-time.After(30 * time.Second):
			s.T.Logf("event-publisher did not stop%s", s.diagnostics())
		}
		s.pubCancel, s.pubDone = nil, nil
	}
	if s.consCancel != nil {
		s.consCancel()
		select {
		case code := <-s.consDone:
			if code != 0 {
				s.T.Logf("event-consumer exit code %d%s", code, s.diagnostics())
			}
		case <-time.After(30 * time.Second):
			s.T.Logf("event-consumer did not stop%s", s.diagnostics())
		}
		s.consCancel, s.consDone = nil, nil
	}
}

// --- seeding -----------------------------------------------------------------

// seedPrerequisites seeds the 007 receive prerequisites through the real
// public functions: caller, API key, grant. The allowlist row is written by
// the real deposit scanner bootstrap (waited for before this runs).
func (s *Scene) seedPrerequisites() {
	s.T.Helper()
	ctx := s.Ctx
	if _, err := s.Pool.Exec(ctx, `INSERT INTO caller (caller_id, label, can_create) VALUES ($1, 'drill', TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create = TRUE`, SceneCaller); err != nil {
		s.T.Fatalf("seed caller: %v", err)
	}
	apiKey, _, err := withdrawal.IssueKey(ctx, s.Pool, SceneCaller, "drill")
	if err != nil {
		s.T.Fatalf("IssueKey: %v", err)
	}
	s.APIKey = apiKey
	if _, err := s.Pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'drill') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, SceneCaller); err != nil {
		s.T.Fatalf("seed execution permission: %v", err)
	}
	// The 011 admission reads the nonce registry scope of the sender; seed the
	// same real prerequisites the authorization supply would establish.
	sender := strings.ToLower(s.Sender.Hex())
	if _, err := s.Pool.Exec(ctx, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1) ON CONFLICT (chain_id, sender) DO UPDATE SET state = 'active'`,
		SceneChainID, sender); err != nil {
		s.T.Fatalf("seed nonce registry: %v", err)
	}
	if _, err := s.Pool.Exec(ctx, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)
		ON CONFLICT (chain_id, sender) DO NOTHING`, SceneChainID, sender); err != nil {
		s.T.Fatalf("seed nonce scope: %v", err)
	}
	if _, err := withdrawal.SupplyGrant(ctx, s.Pool, withdrawal.OpInput{
		OperationID:     "op-drill-supply",
		Action:          "supply",
		AuthorizationID: SceneAuthID,
		CallerID:        SceneCaller,
		ChainID:         SceneChainID,
		Asset:           SceneAsset,
		Recipient:       SceneRecipient,
		Amount:          SceneAmount,
	}, "drill", "controlled drill supply"); err != nil {
		s.T.Fatalf("SupplyGrant: %v", err)
	}
}

// SeedExecutionScope seeds the 011 authorization scope the execution
// admission reads (a test-supplied authorization fact, exactly like the real
// authorization supply; the sender is the chain account).
func (s *Scene) SeedExecutionScope(requestID, intentID, authID string) {
	s.T.Helper()
	if _, err := s.Pool.Exec(s.Ctx, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, 1, 'drill')`,
		authID, intentID, requestID, strings.ToLower(s.Sender.Hex()),
		int64(1e15), int64(2e9), int64(15e8)); err != nil {
		s.T.Fatalf("seed authorization scope: %v", err)
	}
}

// SeedGrant supplies one additional authorization grant through the real
// public operation (one grant per authorization id: the 011 intent uniqueness
// is per authorization).
func (s *Scene) SeedGrant(authID, operationID string) {
	s.T.Helper()
	if _, err := withdrawal.SupplyGrant(s.Ctx, s.Pool, withdrawal.OpInput{
		OperationID:     operationID,
		Action:          "supply",
		AuthorizationID: authID,
		CallerID:        SceneCaller,
		ChainID:         SceneChainID,
		Asset:           SceneAsset,
		Recipient:       SceneRecipient,
		Amount:          SceneAmount,
	}, "drill", "controlled drill supply"); err != nil {
		s.T.Fatalf("SupplyGrant(%s): %v", authID, err)
	}
}

// --- HTTP probes --------------------------------------------------------------

// CreateWithdrawal drives one real POST /withdrawals.
func (s *Scene) CreateWithdrawal(idemKey string) (int, []byte) {
	s.T.Helper()
	body := fmt.Sprintf(
		`{"idempotency_key":%q,"chain_id":%d,"asset":%q,"recipient":%q,"amount":%q,"authorization_id":%q}`,
		idemKey, SceneChainID, SceneAsset, SceneRecipient, SceneAmount, SceneAuthID)
	return s.do(http.MethodPost, "/withdrawals", body)
}

// QueryWithdrawal drives one real GET /withdrawals/{id}.
func (s *Scene) QueryWithdrawal(id string) (int, []byte) {
	s.T.Helper()
	return s.do(http.MethodGet, "/withdrawals/"+id, "")
}

// AdmitExecution drives one real POST /withdrawals/{id}/execution.
func (s *Scene) AdmitExecution(requestID string) (int, []byte) {
	s.T.Helper()
	return s.do(http.MethodPost, "/withdrawals/"+requestID+"/execution", "")
}

// Degradation reads the non-critical status surface.
func (s *Scene) Degradation() (int, map[string]any) {
	s.T.Helper()
	status, raw := s.do(http.MethodGet, "/status/degradation", "")
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	return status, body
}

// do issues one authenticated request against the real listener.
func (s *Scene) do(method, path, body string) (int, []byte) {
	s.T.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, s.BaseURL+path, reader)
	if err != nil {
		s.T.Fatalf("build %s %s: %v", method, path, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+s.APIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.T.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		s.T.Fatalf("read %s %s: %v", method, path, err)
	}
	return resp.StatusCode, raw
}

// --- chain deposits -----------------------------------------------------------

// SendDeposit emits one real Transfer deposit on the local chain and returns
// its transaction hash and mined height.
func (s *Scene) SendDeposit(amount int64) (common.Hash, uint64) {
	s.T.Helper()
	txHash, height, err := s.Anvil.EmitDeposit(s.Sender, common.HexToAddress(SceneAsset), common.HexToAddress(SceneWatch), big.NewInt(amount))
	if err != nil {
		s.T.Fatalf("emit deposit: %v", err)
	}
	return txHash, height
}

// --- durable probes -----------------------------------------------------------

// Count runs one count query.
func (s *Scene) Count(query string, args ...any) int64 {
	s.T.Helper()
	var n int64
	if err := s.Pool.QueryRow(s.Ctx, query, args...).Scan(&n); err != nil {
		s.T.Fatalf("count query: %v", err)
	}
	return n
}

// LedgerRows counts the reference consumer's simulated-ledger effects.
func (s *Scene) LedgerRows() int64 {
	s.T.Helper()
	return s.Count(`SELECT count(*) FROM ` + events.RefLedgerTable)
}

// WaitLedgerRows waits until the reference consumer has applied n events.
func (s *Scene) WaitLedgerRows(n int64) {
	s.T.Helper()
	if err := WaitFor(s.Ctx, 180*time.Second, func() (bool, error) {
		if err := s.checkRuntimes(); err != nil {
			return false, err
		}
		return s.LedgerRows() == n, nil
	}); err != nil {
		s.T.Fatalf("reference ledger rows: %v%s", err, s.diagnostics())
	}
}

// WaitOutbox waits until the outbox totals satisfy the condition.
func (s *Scene) WaitOutbox(timeout time.Duration, what string, cond func(OutboxTotals) bool) OutboxTotals {
	s.T.Helper()
	var totals OutboxTotals
	if err := WaitFor(s.Ctx, timeout, func() (bool, error) {
		if err := s.checkRuntimes(); err != nil {
			return false, err
		}
		var err error
		totals, err = s.Env.OutboxTotals(s.Ctx)
		if err != nil {
			return false, err
		}
		return cond(totals), nil
	}); err != nil {
		s.T.Fatalf("%s: %v%s", what, err, s.diagnostics())
	}
	return totals
}

// WaitLog waits until the captured standard-library log contains substr.
func (s *Scene) WaitLog(timeout time.Duration, substr string) {
	s.T.Helper()
	if err := WaitFor(s.Ctx, timeout, func() (bool, error) {
		return strings.Contains(s.logBuf.String(), substr), nil
	}); err != nil {
		s.T.Fatalf("log evidence %q: %v%s", substr, err, s.diagnostics())
	}
}

// LogContains reports whether the captured log contains substr.
func (s *Scene) LogContains(substr string) bool {
	return strings.Contains(s.logBuf.String(), substr)
}

// ApplyState injects the state and verifies the reachability matches.
func (s *Scene) ApplyState(state StateKind) {
	s.T.Helper()
	if err := s.Env.ApplyState(s.Ctx, state); err != nil {
		s.T.Fatalf("apply state %s: %v", state, err)
	}
	if err := s.Env.VerifyState(s.Ctx, state); err != nil {
		s.T.Fatalf("verify state %s: %v%s", state, err, s.diagnostics())
	}
	s.T.Logf("drill state: %s (%s) redis=%v kafka=%v",
		state, StateLabel[state], s.Env.RedisPing(s.Ctx), s.Env.KafkaReachable(s.Ctx))
}

// RecordState writes one state-level evidence entry (dependency reachability,
// durable snapshot, metric exposition from the real serve listener).
func (s *Scene) RecordState(state StateKind, verdicts map[string]string) {
	s.T.Helper()
	snap, err := s.Env.Snapshot(s.Ctx)
	if err != nil {
		s.T.Fatalf("snapshot at %s: %v", state, err)
	}
	metrics := s.MetricsText()
	if err := s.Env.Evidence.Record("state:"+string(state), map[string]any{
		"state":      state,
		"label":      StateLabel[state],
		"redis":      s.Env.RedisPing(s.Ctx),
		"kafka":      s.Env.KafkaReachable(s.Ctx),
		"snapshot":   snap,
		"verdicts":   verdicts,
		"metrics_ok": metrics != "",
	}); err != nil {
		s.T.Fatalf("record state %s: %v", state, err)
	}
	if metrics != "" {
		if err := s.Env.Evidence.WriteFile("metrics_"+string(state)+".txt", []byte(metrics)); err != nil {
			s.T.Fatalf("write metrics for %s: %v", state, err)
		}
	}
}

// MetricsText scrapes the serve listener's Prometheus exposition.
func (s *Scene) MetricsText() string {
	s.T.Helper()
	resp, err := http.Get(s.BaseURL + "/metrics")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return ""
	}
	return string(raw)
}

// MetricsContains reports whether the serve exposition contains substr.
func (s *Scene) MetricsContains(substr string) bool {
	return strings.Contains(s.MetricsText(), substr)
}

// ConsumerProgressRows counts the durable consumer progress rows.
func (s *Scene) ConsumerProgressRows() int64 {
	s.T.Helper()
	return s.Count(`SELECT count(*) FROM consumer_progress`)
}
