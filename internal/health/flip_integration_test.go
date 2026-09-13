//go:build integration

package health_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	dockercontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/db"
)

// TestReadyzFlipsAndRecoversWithRealDependencies covers FR-012 (SC-003):
// with real PostgreSQL and Anvil, readiness drops within 10s when either
// dependency dies, liveness stays up, and readiness recovers automatically
// within 10s once the dependency returns. /metrics carries the probe counts.
func TestReadyzFlipsAndRecoversWithRealDependencies(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pgCtr := startPostgres(t)
	anvilCtr := startAnvil(t)
	dsn := postgresDSN(t, pgCtr)
	rpcURL := anvilURL(t, anvilCtr)

	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	addr := freeAddr(t)
	base := "http://" + addr
	signals := make(chan os.Signal, 1)
	done := make(chan int, 1)
	go func() {
		done <- app.Serve(ctx, app.Deps{
			Getenv: envMap(map[string]string{
				"TXHARBOR_PG_DSN":           dsn,
				"TXHARBOR_RPC_URL":          rpcURL,
				"TXHARBOR_CHAIN_ID":         "31337",
				"TXHARBOR_START_HEIGHT":     "0",
				"TXHARBOR_LOG_START_HEIGHT": "0",
				"TXHARBOR_LOG_CONTRACTS":    "0x1111111111111111111111111111111111111111",
				"TXHARBOR_HTTP_ADDR":        addr,
			}),
			Stderr:  os.Stderr,
			Signals: signals,
		})
	}()

	waitForStatus(t, base+"/readyz", http.StatusOK, 30*time.Second)
	waitForStatus(t, base+"/livez", http.StatusOK, 5*time.Second)
	if body := httpGet(t, base+"/metrics"); !strings.Contains(body, "txharbor_ready 1") ||
		!strings.Contains(body, "txharbor_probe_total") {
		t.Fatalf("metrics missing foundation metrics:\n%s", body)
	}

	stopTimeout := 5 * time.Second

	// Database outage: readyz 503 within 10s, livez stays 200.
	if err := pgCtr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}
	waitForStatus(t, base+"/readyz", http.StatusServiceUnavailable, 10*time.Second)
	waitForStatus(t, base+"/livez", http.StatusOK, time.Second)
	if body := httpGet(t, base+"/metrics"); !strings.Contains(body, "txharbor_ready 0") {
		t.Fatalf("metrics did not flip to not-ready:\n%s", body)
	}

	// Database recovery: readyz 200 within 10s, no restart.
	if err := pgCtr.Start(ctx); err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	waitForStatus(t, base+"/readyz", http.StatusOK, 10*time.Second)

	// RPC outage and recovery behave the same way.
	if err := anvilCtr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop anvil: %v", err)
	}
	waitForStatus(t, base+"/readyz", http.StatusServiceUnavailable, 10*time.Second)
	waitForStatus(t, base+"/livez", http.StatusOK, time.Second)
	if err := anvilCtr.Start(ctx); err != nil {
		t.Fatalf("start anvil: %v", err)
	}
	waitForStatus(t, base+"/readyz", http.StatusOK, 10*time.Second)

	// Clean signal-driven exit within the 15s budget.
	signals <- syscall.SIGTERM
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("serve exit code = %d, want 0", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not exit within the 15s shutdown budget")
	}
}

func startPostgres(t *testing.T) *postgres.PostgresContainer {
	t.Helper()
	ctx := context.Background()
	// Pin the host port: testcontainers reassigns a random mapped port on
	// container Start, but serve keeps the DSN from startup (as in
	// production, where the address is stable). A fixed port makes
	// stop/start recovery observable on the same address.
	pgPort := fixedPort(t)
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"),
		postgres.WithUsername("txharbor"),
		postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
		testcontainers.WithHostConfigModifier(func(hc *dockercontainer.HostConfig) {
			hc.PortBindings = mustPortMap(t, "5432/tcp", pgPort)
		}),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	return ctr
}

func startAnvil(t *testing.T) testcontainers.Container {
	t.Helper()
	ctx := context.Background()
	// Same fixed-port rationale as startPostgres: the RPC URL is captured
	// at startup and must survive container stop/start.
	rpcPort := fixedPort(t)
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			ExposedPorts: []string{"8545/tcp"},
			// Image entrypoint is /bin/sh -c: override it, otherwise
			// anvil never receives --host and binds 127.0.0.1
			// inside the container (unreachable via port mapping).
			Entrypoint: []string{"anvil"},
			Cmd:        []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
			WaitingFor: wait.ForLog("Listening on"),
			HostConfigModifier: func(hc *dockercontainer.HostConfig) {
				hc.PortBindings = mustPortMap(t, "8545/tcp", rpcPort)
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

func postgresDSN(t *testing.T, ctr *postgres.PostgresContainer) string {
	t.Helper()
	dsn, err := ctr.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

func anvilURL(t *testing.T, ctr testcontainers.Container) string {
	t.Helper()
	ctx := context.Background()
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

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// fixedPort reserves a host port for container port bindings (see
// startPostgres/startAnvil). Small inherent race, acceptable in tests.
func fixedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return strconv.Itoa(port)
}

// mustPortMap binds one container port to a fixed host port (see
// startPostgres/startAnvil).
func mustPortMap(t *testing.T, containerPort, hostPort string) mobynetwork.PortMap {
	t.Helper()
	p, err := mobynetwork.ParsePort(containerPort)
	if err != nil {
		t.Fatalf("parse container port %s: %v", containerPort, err)
	}
	return mobynetwork.PortMap{p: {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort}}}
}

func envMap(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func waitForStatus(t *testing.T, url string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastStatus int
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastStatus = resp.StatusCode
			if resp.StatusCode == want {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("GET %s did not reach %d within %s (last=%d err=%v)", url, want, timeout, lastStatus, lastErr)
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(body)
}
