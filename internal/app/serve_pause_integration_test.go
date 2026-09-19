//go:build integration

// serve_pause_integration_test.go owns Lane-F5's serve-level acceptance: a REAL
// persisted business pause must halt the affected indexer loops, not the
// service. The scene is the one recorded as the defect (Lane-D2): Anvil is
// restarted as a NEW chain behind the same stable address, the scanners
// re-read the diverged identity and persist durable pause rows through their
// own detection paths. With the coordinator classification, the serve process
// keeps listening, readiness stays on the dependency probes, the paused loops
// advance nothing and rewrite no history, the heartbeat keeps the lease (and
// with it the 006 recovery participant) resident, SIGTERM still exits cleanly,
// and a restart stays paused instead of exiting.
//
// No new unpause interface is introduced for the height-0 divergence: it
// remains operator territory, exactly as before.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	dockercontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

const pauseSceneChainID = int64(31337)

// pauseScenePostgres starts PostgreSQL on a pinned host port so the DSN
// captured by Serve survives a stop/start (same rationale as the readiness
// flip test).
func pauseScenePostgres(t *testing.T) *postgres.PostgresContainer {
	t.Helper()
	ctx := context.Background()
	pgPort := pauseSceneFreePort(t)
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
		testcontainers.WithHostConfigModifier(func(hc *dockercontainer.HostConfig) {
			hc.PortBindings = pauseScenePortMap(t, "5432/tcp", pgPort)
		}),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	return ctr
}

// pauseSceneAnvil starts Anvil on a pinned host port so the RPC URL captured
// by Serve survives a stop/start.
func pauseSceneAnvil(t *testing.T) testcontainers.Container {
	t.Helper()
	ctx := context.Background()
	rpcPort := pauseSceneFreePort(t)
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			ExposedPorts: []string{"8545/tcp"},
			Entrypoint:   []string{"anvil"},
			Cmd:          []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
			WaitingFor:   wait.ForLog("Listening on"),
			HostConfigModifier: func(hc *dockercontainer.HostConfig) {
				hc.PortBindings = pauseScenePortMap(t, "8545/tcp", rpcPort)
			},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start anvil: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	return ctr
}

func pauseSceneFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return strconv.Itoa(port)
}

func pauseScenePortMap(t *testing.T, containerPort, hostPort string) mobynetwork.PortMap {
	t.Helper()
	p, err := mobynetwork.ParsePort(containerPort)
	if err != nil {
		t.Fatalf("parse container port %s: %v", containerPort, err)
	}
	return mobynetwork.PortMap{p: {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort}}}
}

// pauseSceneHTTPStatus polls one endpoint until the wanted status is observed.
func pauseSceneHTTPStatus(t *testing.T, url string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last, lastErr := 0, error(nil)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			last, lastErr = resp.StatusCode, nil
			if last == want {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("GET %s never reached %d within %s (last=%d err=%v)", url, want, timeout, last, lastErr)
}

func pauseSceneHTTPBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(body)
}

// pauseSceneAnvilRPC issues one JSON-RPC call with bounded transport retries
// (a just-restarted container may reset the first connection).
func pauseSceneAnvilRPC(t *testing.T, rpcURL, method string, params ...any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		t.Fatalf("anvil %s marshal: %v", method, err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for attempt := 1; ; attempt++ {
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Post(rpcURL, "application/json", bytes.NewReader(body))
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("anvil %s: %v (attempt %d)", method, err, attempt)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		var out struct {
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("anvil %s decode: %v (%s)", method, err, raw)
		}
		if out.Error != nil {
			t.Fatalf("anvil %s error: %s", method, out.Error.Message)
		}
		return
	}
}

func pauseSceneWait(t *testing.T, timeout time.Duration, what string, cond func() bool) {
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

// pauseSceneMetric reads one Prometheus series value (0 when absent).
func pauseSceneMetric(body, series string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, series) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			return v, true
		}
	}
	return 0, false
}

// pauseSceneProgress is the persisted cursor/history/lease snapshot.
type pauseSceneProgress struct {
	headerHeight int64
	logNext      int64
	depositNext  int64
	blocks       string
	logPauses    int64
	indexerPause int64
	depositPause int64
	leaseExpires int64
}

func pauseSceneRead(t *testing.T, pool *pgxpool.Pool) pauseSceneProgress {
	t.Helper()
	ctx := context.Background()
	var p pauseSceneProgress
	p.headerHeight, p.logNext, p.depositNext = -1, -1, -1
	_ = pool.QueryRow(ctx, `SELECT height FROM indexer_checkpoint WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.headerHeight)
	_ = pool.QueryRow(ctx, `SELECT next_block FROM log_checkpoint WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.logNext)
	_ = pool.QueryRow(ctx, `SELECT next_block FROM deposit_checkpoint WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.depositNext)
	var blocks strings.Builder
	rows, err := pool.Query(ctx,
		`SELECT number, hash FROM chain_blocks WHERE chain_id = $1 ORDER BY number`, pauseSceneChainID)
	if err != nil {
		t.Fatalf("read chain_blocks: %v", err)
	}
	for rows.Next() {
		var number int64
		var hash string
		if err := rows.Scan(&number, &hash); err != nil {
			rows.Close()
			t.Fatalf("scan chain_blocks: %v", err)
		}
		fmt.Fprintf(&blocks, "%d=%s;", number, hash)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("chain_blocks rows: %v", err)
	}
	p.blocks = blocks.String()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM log_pause WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.logPauses); err != nil {
		t.Fatalf("read log_pause: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM indexer_pause WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.indexerPause); err != nil {
		t.Fatalf("read indexer_pause: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_pause WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.depositPause); err != nil {
		t.Fatalf("read deposit_pause: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT extract(epoch FROM expires_at)::bigint FROM indexer_lease WHERE chain_id = $1`, pauseSceneChainID).Scan(&p.leaseExpires); err != nil {
		p.leaseExpires = -1
	}
	return p
}

// pauseSceneRequireFrozen asserts the persisted progress/history does not move
// across a bounded observation window (no busy retry, no advance, no history
// rewrite while paused). The lease expiry is excluded: the heartbeat must keep
// advancing it while the coordinator stays resident.
func pauseSceneRequireFrozen(t *testing.T, pool *pgxpool.Pool, want pauseSceneProgress, window time.Duration) {
	t.Helper()
	want.leaseExpires = 0
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		got := pauseSceneRead(t, pool)
		got.leaseExpires = 0
		if got != want {
			t.Fatalf("progress moved while paused:\n want %+v\n  got %+v", want, got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestServeDurablePauseKeepsServiceAliveAndPaused(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pgCtr := pauseScenePostgres(t)
	anvilCtr := pauseSceneAnvil(t)
	dsn := postgresDSN(t, pgCtr)
	rpcURL := anvilURL(t, anvilCtr)
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	addr := freeAddr(t)
	base := "http://" + addr
	sceneEnv := map[string]string{
		"TXHARBOR_PG_DSN":                  dsn,
		"TXHARBOR_RPC_URL":                 rpcURL,
		"TXHARBOR_CHAIN_ID":                "31337",
		"TXHARBOR_START_HEIGHT":            "0",
		"TXHARBOR_LOG_START_HEIGHT":        "0",
		"TXHARBOR_LOG_CONTRACTS":           "0x2222222222222222222222222222222222222222",
		"TXHARBOR_DEPOSIT_START_HEIGHT":    "0",
		"TXHARBOR_DEPOSIT_CONTRACTS":       "0x2222222222222222222222222222222222222222:0",
		"TXHARBOR_DEPOSIT_WATCH_ADDRESSES": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
		"TXHARBOR_CONFIRMATION_DEPTH":      "10",
		"TXHARBOR_REORG_MAX_DEPTH":         "25",
		"TXHARBOR_HTTP_ADDR":               addr,
	}

	type serveHandle struct {
		signals chan os.Signal
		done    chan int
		stopped bool
	}
	var stderr bytes.Buffer
	startServe := func() *serveHandle {
		h := &serveHandle{signals: make(chan os.Signal, 1), done: make(chan int, 1)}
		go func() {
			h.done <- Serve(ctx, Deps{Getenv: envGetter(sceneEnv), Stderr: &stderr, Signals: h.signals})
		}()
		return h
	}
	stopServe := func(h *serveHandle) {
		if h.stopped {
			return
		}
		select {
		case code := <-h.done:
			h.stopped = true
			if code != 0 {
				t.Fatalf("Serve exit code = %d, want 0 (paused shutdown)", code)
			}
			return
		default:
		}
		select {
		case h.signals <- os.Interrupt:
		default:
		}
		select {
		case code := <-h.done:
			h.stopped = true
			if code != 0 {
				t.Fatalf("Serve exit code = %d, want 0 (paused shutdown)", code)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Serve did not exit after SIGTERM while paused")
		}
	}

	first := startServe()
	defer stopServe(first)

	pauseSceneHTTPStatus(t, base+"/readyz", http.StatusOK, 60*time.Second)
	pauseSceneHTTPStatus(t, base+"/livez", http.StatusOK, 5*time.Second)

	// Both scanners consume the genesis block.
	pauseSceneWait(t, 60*time.Second, "scanners reached genesis", func() bool {
		p := pauseSceneRead(t, pool)
		return p.blocks != "" && p.logNext >= 1
	})

	// The old chain advances to height 1 and both scanners consume it, so the
	// divergence below happens at an already-scanned height.
	pauseSceneAnvilRPC(t, rpcURL, "anvil_mine", "0x1")
	pauseSceneWait(t, 60*time.Second, "scanners consumed height 1", func() bool {
		p := pauseSceneRead(t, pool)
		return p.headerHeight >= 1 && p.logNext >= 2 && strings.Contains(p.blocks, "1=")
	})

	// REAL trigger (Lane-D2 scenario): Anvil restarts as a NEW chain behind the
	// same stable address; two new blocks force the scanners to re-read the
	// diverged identity and persist a durable pause through their own paths.
	stopTimeout := 5 * time.Second
	if err := anvilCtr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop anvil: %v", err)
	}
	if err := anvilCtr.Start(ctx); err != nil {
		t.Fatalf("start anvil: %v", err)
	}
	pauseSceneAnvilRPC(t, rpcURL, "anvil_mine", "0x2")

	pauseSceneWait(t, 90*time.Second, "durable pause persisted", func() bool {
		p := pauseSceneRead(t, pool)
		return p.logPauses > 0 || p.indexerPause > 0
	})

	// 1) The persisted cause and the paused scanner state are real.
	var (
		kind    string
		height  int64
		detail  string
		pauseIn string
	)
	if err := pool.QueryRow(ctx,
		`SELECT kind, height, detail FROM log_pause WHERE chain_id = $1`,
		pauseSceneChainID).Scan(&kind, &height, &detail); err == nil {
		pauseIn = "log_pause"
	} else if err := pool.QueryRow(ctx,
		`SELECT kind, height, detail FROM indexer_pause WHERE chain_id = $1`,
		pauseSceneChainID).Scan(&kind, &height, &detail); err == nil {
		pauseIn = "indexer_pause"
	} else {
		t.Fatalf("no pause row readable after the persisted pause: %v", err)
	}
	if kind == "" || detail == "" {
		t.Fatalf("pause row = %s kind=%q height=%d detail=%q, want a persisted cause", pauseIn, kind, height, detail)
	}
	t.Logf("durable pause persisted in %s: kind=%s height=%d detail=%s", pauseIn, kind, height, detail)

	pauseSceneWait(t, 30*time.Second, "paused scanner state metric", func() bool {
		body := pauseSceneHTTPBody(t, base+"/metrics")
		for _, series := range []string{
			`txharbor_log_state{chain="31337"}`,
			`txharbor_indexer_state{chain="31337"}`,
		} {
			if v, ok := pauseSceneMetric(body, series); ok && v == 3 {
				return true
			}
		}
		return false
	})

	// Cursor and history freeze; no busy retry writes anything.
	frozen := pauseSceneRead(t, pool)
	pauseSceneRequireFrozen(t, pool, frozen, 3*time.Second)

	// 2) Healthy dependencies do not flip readiness; liveness and the
	// observability route stay available.
	pauseSceneHTTPStatus(t, base+"/readyz", http.StatusOK, 5*time.Second)
	pauseSceneHTTPStatus(t, base+"/livez", http.StatusOK, 5*time.Second)
	if body := pauseSceneHTTPBody(t, base+"/metrics"); !strings.Contains(body, "txharbor_log_state") {
		t.Fatalf("metrics route unavailable while paused:\n%.200s", body)
	}

	// The coordinator heartbeat keeps the lease (and thus the resident 006
	// recovery participant) alive while paused.
	leaseBefore := frozen.leaseExpires
	pauseSceneWait(t, 30*time.Second, "lease heartbeat while paused", func() bool {
		p := pauseSceneRead(t, pool)
		return p.leaseExpires > leaseBefore
	})

	// A real dependency outage still flips readiness per the original contract.
	if err := pgCtr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}
	pauseSceneHTTPStatus(t, base+"/readyz", http.StatusServiceUnavailable, 15*time.Second)
	pauseSceneHTTPStatus(t, base+"/livez", http.StatusOK, 2*time.Second)
	if err := pgCtr.Start(ctx); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	pauseSceneHTTPStatus(t, base+"/readyz", http.StatusOK, 20*time.Second)

	// 4) SIGTERM exits within the original shutdown budget while paused.
	stopServe(first)

	// 3) A restart observes the pause and stays paused and resident — it never
	// advances on its own (no new unpause permission for the height-0
	// divergence was invented).
	second := startServe()
	defer stopServe(second)

	pauseSceneHTTPStatus(t, base+"/readyz", http.StatusOK, 60*time.Second)
	restarted := pauseSceneRead(t, pool)
	if restarted.logPauses == 0 && restarted.indexerPause == 0 {
		t.Fatal("pause rows disappeared across the restart")
	}
	pauseSceneRequireFrozen(t, pool, restarted, 3*time.Second)
	pauseSceneHTTPStatus(t, base+"/livez", http.StatusOK, 5*time.Second)
	t.Logf("restart stayed paused and resident: log_pause=%d indexer_pause=%d log_next=%d header=%d",
		restarted.logPauses, restarted.indexerPause, restarted.logNext, restarted.headerHeight)
	stopServe(second)
}
