//go:build integration

// helpers_integration_test.go owns the disposable local stack for the
// jointwire integration proofs: a migrated PostgreSQL container and an Anvil
// node. Both are testcontainers-only and never a deployment target.
package jointwire

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
)

// startMigratedPG starts a disposable PostgreSQL with the joint migration set
// applied, so the worker's real SQL runs against the real schema.
func startMigratedPG(t *testing.T) string {
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
		t.Fatalf("connection string: %v", err)
	}
	opts := db.MigrateOptions{DSN: dsn, LockTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second}
	if err := db.MigrateUp(ctx, opts, io.Discard); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	return dsn
}

// startAnvil starts a disposable Anvil node and returns its HTTP URL.
func startAnvil(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			ExposedPorts: []string{"8545/tcp"},
			Entrypoint:   []string{"anvil"},
			Cmd:          []string{"--host", "0.0.0.0", "--port", "8545", "--chain-id", "31337"},
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

// entryConfig is the direct-construction configuration for the assembly test
// (the live entry test goes through config.Load instead).
func entryConfig(pgdsn, rpcURL string) *config.Config {
	return &config.Config{
		PGDSN:              pgdsn,
		RPCURL:             rpcURL,
		ChainID:            31337,
		ProbeTimeout:       5 * time.Second,
		TxSignerURL:        "http://127.0.0.1:1",
		TxSignerCredential: "jointwire-test-credential",
		TxSendTimeout:      5 * time.Second,
		NonceReadToken:     "jointwire-test-token",
		WorkerTTL:          config.DefaultWorkerTTL,
		WorkerHeartbeat:    config.DefaultWorkerHeartbeat,
		WorkerStall:        config.DefaultWorkerStall,
		WorkerBackoffBase:  config.DefaultWorkerBackoffBase,
		WorkerBackoffMax:   config.DefaultWorkerBackoffMax,
		WorkerScanInterval: config.DefaultWorkerScanInterval,
		WorkerLabel:        "jointwire-test",
	}
}

// entryEnv is the minimal valid process environment for `withdrawal-worker`
// against the disposable stack.
func entryEnv(pgdsn, rpcURL string) map[string]string {
	return map[string]string{
		config.EnvPGDSN:                 pgdsn,
		config.EnvRPCURL:                rpcURL,
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
		config.EnvTxSignerURL:           "http://127.0.0.1:1",
		config.EnvTxSignerCredential:    "jointwire-test-credential",
		config.EnvNonceReadToken:        "jointwire-test-token",
	}
}

func fakeEnv(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

// syncBuffer is a goroutine-safe writer for capturing command output while the
// command runs.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitForText blocks until buf contains want or the timeout expires.
func waitForText(t *testing.T, buf *syncBuffer, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if bytes.Contains([]byte(buf.String()), []byte(want)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("output did not contain %q within %s; output=%s", want, timeout, buf.String())
}
