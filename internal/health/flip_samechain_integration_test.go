//go:build integration

// flip_samechain_integration_test.go owns the Lane-F4 same-chain RPC fault
// injection helpers for TestReadyzFlipsAndRecoversWithRealDependencies.
//
// The RPC outage leg must interrupt the CONNECTION, not the chain: the Anvil
// process is frozen in place (SIGSTOP / SIGCONT via a container exec), so the
// container, its stable address and its in-memory chain survive and only RPC
// traffic stalls. The previous stop/start injection restarted a NEW chain
// (fresh genesis), which the indexer correctly pauses on — that production
// contract question is recorded separately and is neither fixed nor endorsed
// here; this file only corrects the connection-recovery test precondition.
//
// All helpers are bounded: RPC calls carry a context deadline, DB reads a 5s
// timeout, and the freeze release is idempotent and runs on failure paths too.
package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// flipChainID is the chain id configured by this scene (TXHARBOR_CHAIN_ID).
const flipChainID = int64(31337)

// flipChainIdentity is the observable chain + persisted scan identity used for
// the before/after same-chain continuity assertions.
type flipChainIdentity struct {
	chainIDHex string
	block0Hash string
	blocks     []string // "number=hash", ordered
	checkpoint string   // "start=..,next=..,config=.." or "absent"
	logPause   int
}

// flipRPCWithin is one bounded real JSON-RPC call (returns the raw result).
func flipRPCWithin(rpcURL string, timeout time.Duration, method string, params ...any) (string, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", err
	}
	if out.Error != nil {
		return "", fmt.Errorf("rpc error: %s", out.Error.Message)
	}
	return string(out.Result), nil
}

// readFlipChainIdentity reads the live chain identity (chain id + block 0 hash)
// and the persisted scan identity (chain_blocks rows, log_checkpoint row,
// log_pause count) with bounded, read-only reads.
func readFlipChainIdentity(t *testing.T, rpcURL, dsn string) flipChainIdentity {
	t.Helper()
	var id flipChainIdentity
	rawChain, err := flipRPCWithin(rpcURL, 5*time.Second, "eth_chainId")
	if err != nil {
		t.Fatalf("read chain identity: eth_chainId: %v", err)
	}
	if err := json.Unmarshal([]byte(rawChain), &id.chainIDHex); err != nil {
		t.Fatalf("read chain identity: decode chain id %q: %v", rawChain, err)
	}
	rawBlock, err := flipRPCWithin(rpcURL, 5*time.Second, "eth_getBlockByNumber", "0x0", false)
	if err != nil {
		t.Fatalf("read chain identity: eth_getBlockByNumber(0): %v", err)
	}
	var block struct {
		Hash string `json:"hash"`
	}
	if err := json.Unmarshal([]byte(rawBlock), &block); err != nil {
		t.Fatalf("read chain identity: decode block 0: %v", err)
	}
	id.block0Hash = block.Hash

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("read chain identity: pg connect: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx,
		`SELECT number, hash FROM chain_blocks WHERE chain_id = $1 ORDER BY number`, flipChainID)
	if err != nil {
		t.Fatalf("read chain identity: chain_blocks: %v", err)
	}
	for rows.Next() {
		var number int64
		var hash string
		if err := rows.Scan(&number, &hash); err != nil {
			rows.Close()
			t.Fatalf("read chain identity: scan chain_blocks: %v", err)
		}
		id.blocks = append(id.blocks, fmt.Sprintf("%d=%s", number, hash))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("read chain identity: chain_blocks rows: %v", err)
	}

	var startBlock, nextBlock int64
	var configHash string
	err = conn.QueryRow(ctx,
		`SELECT start_block, next_block, config_hash FROM log_checkpoint WHERE chain_id = $1`,
		flipChainID).Scan(&startBlock, &nextBlock, &configHash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		id.checkpoint = "absent"
	case err != nil:
		t.Fatalf("read chain identity: log_checkpoint: %v", err)
	default:
		id.checkpoint = fmt.Sprintf("start=%d,next=%d,config=%s", startBlock, nextBlock, configHash)
	}

	if err := conn.QueryRow(ctx,
		`SELECT count(*) FROM log_pause WHERE chain_id = $1`, flipChainID).Scan(&id.logPause); err != nil {
		t.Fatalf("read chain identity: log_pause count: %v", err)
	}
	return id
}

// flipContainerID exposes the container id for the Docker API pause/unpause.
func flipContainerID(t *testing.T, ctr testcontainers.Container) string {
	t.Helper()
	ider, ok := ctr.(interface{ GetContainerID() string })
	if !ok {
		t.Fatalf("container %T does not expose GetContainerID", ctr)
	}
	return ider.GetContainerID()
}

// flipPauseAnvil freezes the Anvil container through the Docker API
// (docker pause): the process and its in-memory chain stay intact and only RPC
// traffic stalls. In-container SIGSTOP cannot be used — PID 1 in a container's
// PID namespace is protected from stop signals sent inside that namespace. The
// returned client is used to unpause on every path, including failures.
func flipPauseAnvil(t *testing.T, ctr testcontainers.Container) client.APIClient {
	t.Helper()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatalf("pause anvil: docker provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	cli := provider.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.ContainerPause(ctx, flipContainerID(t, ctr), client.ContainerPauseOptions{}); err != nil {
		t.Fatalf("pause anvil container: %v", err)
	}
	return cli
}

// flipUnpauseAnvil resumes the container. It returns (never raises) the error
// so failure paths can use it as a best-effort release.
func flipUnpauseAnvil(cli client.APIClient, ctr testcontainers.Container) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := cli.ContainerUnpause(ctx, flipContainerIDNoT(ctr), client.ContainerUnpauseOptions{})
	return err
}

// flipContainerIDNoT is the non-fatal container-id read for failure paths.
func flipContainerIDNoT(ctr testcontainers.Container) string {
	if ider, ok := ctr.(interface{ GetContainerID() string }); ok {
		return ider.GetContainerID()
	}
	return ""
}

// flipRPCBlocked asserts the real RPC endpoint is stalled: a bounded real call
// must fail within its own deadline while the process is frozen. It never
// fakes a probe result — it observes the real transport.
func flipRPCBlocked(t *testing.T, rpcURL string, timeout time.Duration) {
	t.Helper()
	start := time.Now()
	if _, err := flipRPCWithin(rpcURL, timeout, "eth_chainId"); err == nil {
		t.Fatalf("paused anvil answered eth_chainId within %s; real RPC was not stalled", timeout)
	} else {
		t.Logf("paused anvil: real eth_chainId failed after %s (%v)", time.Since(start).Round(time.Millisecond), err)
	}
}

// flipReadyzStatus performs one bounded readiness GET and returns the status
// plus a bounded body snippet.
func flipReadyzStatus(t *testing.T, base string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", nil)
	if err != nil {
		t.Fatalf("readyz request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(body))
}

// flipProbeSeries matches one txharbor_probe_total series value.
var flipProbeSeries = regexp.MustCompile(`txharbor_probe_total\{dep="([^"]+)",result="([^"]+)"\}\s+([0-9.eE+]+)`)

// readFlipProbeCounter reads the txharbor_probe_total value for one dep/result
// series (0 when the series has not appeared yet).
func readFlipProbeCounter(t *testing.T, base, dep, result string) float64 {
	t.Helper()
	body := httpGet(t, base+"/metrics")
	for _, m := range flipProbeSeries.FindAllStringSubmatch(body, -1) {
		if m[1] != dep || m[2] != result {
			continue
		}
		v, err := strconv.ParseFloat(m[3], 64)
		if err != nil {
			t.Fatalf("parse probe counter %q: %v", m[0], err)
		}
		return v
	}
	return 0
}

// waitFlipProbeAdvance observes real probe-cycle progression: the named probe
// series must strictly increase within the bounded deadline. It returns the
// new value and whether progression was observed (caller decides the failure
// message so the serve-exit reason can be included).
func waitFlipProbeAdvance(t *testing.T, base, dep, result string, from float64, timeout time.Duration) (float64, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v := readFlipProbeCounter(t, base, dep, result); v > from {
			return v, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return readFlipProbeCounter(t, base, dep, result), false
}

// flipServeExit records the serve exit without consuming the exit-code channel
// the shutdown leg reads. alive() is a non-blocking observation.
type flipServeExit struct {
	mu   sync.Mutex
	code int
	at   time.Time
	done chan struct{}
}

func newFlipServeExit() *flipServeExit { return &flipServeExit{done: make(chan struct{})} }

func (s *flipServeExit) mark(code int) {
	s.mu.Lock()
	s.code, s.at = code, time.Now()
	s.mu.Unlock()
	close(s.done)
}

func (s *flipServeExit) alive() bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

func (s *flipServeExit) reason() string {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return fmt.Sprintf("serve exited early code=%d at %s", s.code, s.at.Format(time.RFC3339Nano))
	default:
		return "serve alive"
	}
}
