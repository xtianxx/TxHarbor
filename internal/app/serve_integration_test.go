//go:build integration

package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
)

// TestServeConfigErrorsEndToEnd covers US2 (SC-002): missing and invalid
// configuration both terminate with a diagnostic naming the variable and a
// non-zero exit, before serving.
func TestServeConfigErrorsEndToEnd(t *testing.T) {
	addr := freeAddr(t)
	base := map[string]string{
		"TXHARBOR_PG_DSN":           "postgres://txharbor:txharbor@127.0.0.1:5432/txharbor?sslmode=disable",
		"TXHARBOR_RPC_URL":          "http://127.0.0.1:8545",
		"TXHARBOR_CHAIN_ID":         "31337",
		"TXHARBOR_START_HEIGHT":     "0",
		"TXHARBOR_LOG_START_HEIGHT": "0",
		"TXHARBOR_LOG_CONTRACTS":    "0x1111111111111111111111111111111111111111",
		"TXHARBOR_HTTP_ADDR":        addr,
	}
	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantVar string
	}{
		{name: "missing pg dsn", mutate: func(m map[string]string) { delete(m, "TXHARBOR_PG_DSN") }, wantVar: "TXHARBOR_PG_DSN"},
		{name: "invalid chain id", mutate: func(m map[string]string) { m["TXHARBOR_CHAIN_ID"] = "abc" }, wantVar: "TXHARBOR_CHAIN_ID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			tc.mutate(env)

			var stderr bytes.Buffer
			code := Serve(context.Background(), Deps{
				Getenv:  envGetter(env),
				Stderr:  &stderr,
				Signals: make(chan os.Signal),
			})
			if code == 0 {
				t.Fatalf("Serve() exit code = 0, want non-zero")
			}
			if !strings.Contains(stderr.String(), tc.wantVar) {
				t.Fatalf("stderr does not name %s: %s", tc.wantVar, stderr.String())
			}
		})
	}
}

// TestServeRefusesUnmigratedDatabaseEndToEnd covers US1-4 / FR-008: serve
// never auto-migrates; with pending migrations it exits non-zero and points
// at `migrate up`.
func TestServeRefusesUnmigratedDatabaseEndToEnd(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pgCtr := startPostgresContainer(t)
	dsn := postgresDSN(t, pgCtr)
	addr := freeAddr(t)

	var stderr bytes.Buffer
	code := Serve(ctx, Deps{
		Getenv: envGetter(map[string]string{
			"TXHARBOR_PG_DSN":           dsn,
			"TXHARBOR_RPC_URL":          "http://127.0.0.1:1",
			"TXHARBOR_CHAIN_ID":         "31337",
			"TXHARBOR_START_HEIGHT":     "0",
			"TXHARBOR_LOG_START_HEIGHT": "0",
			"TXHARBOR_LOG_CONTRACTS":    "0x1111111111111111111111111111111111111111",
			"TXHARBOR_HTTP_ADDR":        addr,
		}),
		Stderr:  &stderr,
		Signals: make(chan os.Signal),
	})
	if code == 0 {
		t.Fatal("Serve() exit code = 0, want non-zero for pending migrations")
	}
	if !strings.Contains(stderr.String(), "pending") || !strings.Contains(stderr.String(), "migrate up") {
		t.Fatalf("stderr lacks pending-migration diagnosis: %s", stderr.String())
	}
}

// TestServeWrongChainEndToEnd covers US4-1 (SC-004): a real Anvil reporting
// chain 31337 is rejected when the operator expects chain 1, and the error
// states both values.
func TestServeWrongChainEndToEnd(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	pgCtr := startPostgresContainer(t)
	anvilCtr := startAnvilContainer(t)
	dsn := postgresDSN(t, pgCtr)
	rpcURL := anvilURL(t, anvilCtr)

	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 5 * time.Second, ConnectTimeout: 5 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	var stderr bytes.Buffer
	code := Serve(ctx, Deps{
		Getenv: envGetter(map[string]string{
			"TXHARBOR_PG_DSN":           dsn,
			"TXHARBOR_RPC_URL":          rpcURL,
			"TXHARBOR_CHAIN_ID":         "1",
			"TXHARBOR_START_HEIGHT":     "0",
			"TXHARBOR_LOG_START_HEIGHT": "0",
			"TXHARBOR_LOG_CONTRACTS":    "0x1111111111111111111111111111111111111111",
			"TXHARBOR_HTTP_ADDR":        freeAddr(t),
		}),
		Stderr:  &stderr,
		Signals: make(chan os.Signal),
	})
	if code == 0 {
		t.Fatal("Serve() exit code = 0, want non-zero for wrong chain")
	}
	msg := stderr.String()
	if !strings.Contains(msg, "expected 1") || !strings.Contains(msg, "actual 31337") {
		t.Fatalf("stderr does not report expected/actual chain ids: %s", msg)
	}
}

func startPostgresContainer(t *testing.T) *postgres.PostgresContainer {
	t.Helper()
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
	return ctr
}

func startAnvilContainer(t *testing.T) testcontainers.Container {
	t.Helper()
	ctx := context.Background()
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

func envGetter(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
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
