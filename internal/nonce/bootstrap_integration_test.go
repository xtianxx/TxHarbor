//go:build integration

// bootstrap_integration_test.go owns spec task T028 for 008-nonce-manager:
// the V6 bootstrap-evidence E2E (quickstart V6; FR-12/R2; SC-06) over a real
// PostgreSQL plus a real Anvil (the `logscanStartAnvilNode` precedent: foundry
// v1.8.1, chain 31337).
//
// A first-ever scope whose sender already has chain history must NOT have that
// consumption silently merged into the sequence. The Anvil default account is
// given N mined transactions BEFORE the scope's first allocation, so the chain
// reports latest = pending = P = N > 0. The carrier-level admission
// (nonce.NewAllocator) then persists a `bootstrap_external_consumed`
// observation covering [0, P), admits the intent at P, creates no hold, and
// leaves the evidence queryable through the binding's allocation_observation_id.
package nonce_test

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	// bootChainID is the Anvil container's chain id (--chain-id 31337).
	bootChainID = int64(31337)
	// bootPreMinedTxns is P: the pre-existing chain history the sender carries
	// before the scope's first-ever allocation.
	bootPreMinedTxns = uint64(3)
	// bootAuthorization is the active 007 authorization the admission binds.
	bootAuthorization = "wa-bootstrap-evidence"
)

// bootAnvilNode is the real chain truth: a pinned Anvil container plus the
// JSON-RPC client and the unlocked default accounts.
type bootAnvilNode struct {
	rpc      *rpc.Client
	accounts []common.Address
}

// bootStartAnvilNode boots the pinned Anvil image (v1.8.1, chain 31337).
// Skips (never passes) without a healthy Docker provider.
func bootStartAnvilNode(t *testing.T) *bootAnvilNode {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "ghcr.io/foundry-rs/foundry:v1.8.1",
			ExposedPorts: []string{"8545/tcp"},
			// The image entrypoint is /bin/sh -c; without the override anvil
			// never receives --host and binds 127.0.0.1 inside the container.
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
	c, err := rpc.DialContext(ctx, fmt.Sprintf("http://%s:%s", host, port.Port()))
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
	return &bootAnvilNode{rpc: c, accounts: accounts}
}

func (n *bootAnvilNode) mustCall(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.rpc.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

func (n *bootAnvilNode) blockNumber(t *testing.T) uint64 {
	t.Helper()
	var num hexutil.Uint64
	n.mustCall(t, &num, "eth_blockNumber")
	return uint64(num)
}

func (n *bootAnvilNode) txCount(t *testing.T, sender, block string) uint64 {
	t.Helper()
	var count hexutil.Uint64
	n.mustCall(t, &count, "eth_getTransactionCount", sender, block)
	return uint64(count)
}

// sendMinedTx broadcasts one zero-value transfer from the first default account
// and waits until automine has mined it (a new head exists).
func (n *bootAnvilNode) sendMinedTx(t *testing.T, to common.Address) common.Hash {
	t.Helper()
	before := n.blockNumber(t)
	var txHash common.Hash
	n.mustCall(t, &txHash, "eth_sendTransaction", map[string]any{
		"from": n.accounts[0], "to": to, "gas": "0x5208",
	})
	deadline := time.Now().Add(10 * time.Second)
	for n.blockNumber(t) <= before {
		if time.Now().After(deadline) {
			t.Fatal("anvil did not mine the broadcast transaction (automine disabled?)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return txHash
}

// sendPendingTx broadcasts one zero-value transfer and returns immediately,
// without waiting for a head: used with automine disabled to stage a real
// pending-only spike (pending > latest).
func (n *bootAnvilNode) sendPendingTx(t *testing.T, from, to common.Address) common.Hash {
	t.Helper()
	var txHash common.Hash
	n.mustCall(t, &txHash, "eth_sendTransaction", map[string]any{
		"from": from, "to": to, "gas": "0x5208",
	})
	return txHash
}

// bootDisableAutomine turns off automine so a broadcast transaction stays
// pending; the chain then reports latest < pending for the sender.
func (n *bootAnvilNode) bootDisableAutomine(t *testing.T) {
	t.Helper()
	var ignored any
	n.mustCall(t, &ignored, "evm_setAutomine", false)
}

// bootAssertCounts waits for the chain to report exactly latest/pending for the
// sender, so the observation's pre-existing history is real and stable.
func bootAssertCounts(t *testing.T, n *bootAnvilNode, sender string, latest, pending uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		gotLatest := n.txCount(t, sender, "latest")
		gotPending := n.txCount(t, sender, "pending")
		if gotLatest == latest && gotPending == pending {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("chain view latest=%d pending=%d, want %d/%d", gotLatest, gotPending, latest, pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// bootMigratedPool boots an isolated scratch PostgreSQL and applies the
// embedded 000001-000008 migrations as one goose up.
func bootMigratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := nonceStartPostgres(t)
	if err := db.MigrateUp(context.Background(), nonceMigrateOptions(dsn), &bootDiscard{}); err != nil {
		t.Fatalf("MigrateUp: %v", err)
	}
	pool, err := db.OpenPool(context.Background(), dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// bootDiscard swallows migration output (the assertions live in the database).
type bootDiscard struct{}

func (*bootDiscard) Write(p []byte) (int, error) { return len(p), nil }

// bootSeedScope registers the Anvil default account and supplies one active
// 007 authorization so admission reaches the binding insert.
func bootSeedScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) {
	t.Helper()
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("exec %q: %v", query, err)
		}
	}
	mustExec(`INSERT INTO caller (caller_id, label) VALUES (1, 'bootstrap-it')`)
	mustExec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, bootChainID, sender)
	mustExec(`INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`,
		bootAuthorization, bootChainID, sender, sender)
}

// bootObservation is one persisted evidence row, raw columns only.
type bootObservation struct {
	kind           string
	classification string
	latest         string
	pending        string
}

func bootReadObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, observationID string) bootObservation {
	t.Helper()
	var o bootObservation
	if err := pool.QueryRow(ctx, `
SELECT kind, classification, COALESCE(latest_count::text, ''), COALESCE(pending_count::text, '')
FROM nonce_observations WHERE observation_id = $1`, observationID).
		Scan(&o.kind, &o.classification, &o.latest, &o.pending); err != nil {
		t.Fatalf("read observation %s: %v", observationID, err)
	}
	return o
}

func bootCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// TestNonceBootstrapExternalConsumedE2E is the T028 V6 acceptance: a
// first-ever scope with pre-existing chain history records
// `bootstrap_external_consumed` evidence for [0, P), admits at P with no hold,
// and leaves the evidence queryable — never a silent merge.
func TestNonceBootstrapExternalConsumedE2E(t *testing.T) {
	ctx := context.Background()
	pool := bootMigratedPool(t)

	anvil := bootStartAnvilNode(t)
	sender := strings.ToLower(anvil.accounts[0].Hex())
	bootSeedScope(t, ctx, pool, sender)

	// The scope has never been seen locally, but the sender already consumed N
	// nonces on chain: pre-mine them (automine) so latest = pending = P = N > 0
	// BEFORE the scope's first allocation.
	burn := common.HexToAddress("0x000000000000000000000000000000000000dead")
	for i := uint64(0); i < bootPreMinedTxns; i++ {
		anvil.sendMinedTx(t, burn)
	}
	bootAssertCounts(t, anvil, sender, bootPreMinedTxns, bootPreMinedTxns)

	observer := nonce.NewObserver(anvil.rpc, nonce.ObserverConfig{
		RPCTimeout:   5 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	alloc := nonce.NewAllocator(pool, observer, admitGate)
	req := nonce.AllocationRequest{
		IntentID:        "boot-intent-first-scope",
		ChainID:         bootChainID,
		Sender:          sender,
		AuthorizationID: bootAuthorization,
	}

	binding, outcome, err := alloc.Allocate(ctx, req)
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("bootstrap allocate = (%+v, %q, %v), want allocated", binding, outcome, err)
	}

	// Admission at P — the pre-existing [0, P) is never handed out again.
	p := new(big.Int).SetUint64(bootPreMinedTxns)
	if binding.Nonce.Cmp(p) != 0 {
		t.Fatalf("admitted nonce = %s, want P = %s", binding.Nonce, p)
	}

	// The observation is explicitly bootstrap_external_consumed (kind
	// allocation), covering [0, P): the persisted pending count is exactly P,
	// so the consumed range is recorded, auditable evidence.
	obs := bootReadObservation(t, ctx, pool, binding.AllocationObservationID)
	if obs.kind != "allocation" {
		t.Fatalf("observation kind = %q, want allocation", obs.kind)
	}
	if obs.classification != "bootstrap_external_consumed" {
		t.Fatalf("observation classification = %q, want bootstrap_external_consumed", obs.classification)
	}
	if obs.pending != p.String() {
		t.Fatalf("observation pending_count = %q, want P = %s", obs.pending, p)
	}

	// No binding was silently created inside the consumed range.
	if n := bootCount(t, ctx, pool,
		`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2 AND nonce < $3`,
		bootChainID, sender, p.String()); n != 0 {
		t.Fatalf("%d binding(s) below P — the pre-existing history was silently merged", n)
	}

	// Healthy bootstrap is explicitly not a hold.
	if n := bootCount(t, ctx, pool,
		`SELECT count(*) FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2`,
		bootChainID, sender); n != 0 {
		t.Fatalf("bootstrap created %d hold(s), want 0", n)
	}

	// The evidence is queryable and linked to the admitted binding: the
	// binding's allocation_observation_id resolves to the bootstrap row whose
	// pending count is the bound nonce (candidate == P).
	if n := bootCount(t, ctx, pool, `
SELECT count(*) FROM nonce_bindings b
JOIN nonce_observations o ON o.observation_id = b.allocation_observation_id
WHERE b.chain_id = $1 AND b.sender = $2
  AND o.classification = 'bootstrap_external_consumed'
  AND b.nonce = o.pending_count`, bootChainID, sender); n != 1 {
		t.Fatalf("queryable bootstrap evidence rows = %d, want 1", n)
	}
	if n := bootCount(t, ctx, pool,
		`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2`,
		bootChainID, sender); n != 1 {
		t.Fatalf("scope bindings = %d, want exactly 1", n)
	}
}

// TestNonceBootstrapPendingOnlySpikeAdmitsAtP pins the approved fresh-scope
// bootstrap policy for the pending-only spike the matrix can produce: a
// first-ever scope whose sender has queued but unmined transactions reports
// 0 = latest < pending = P, and the approved behavior is to record
// bootstrap_external_consumed for [0, P) and admit at P with no hold
// (contracts/observation.md §2: a first-time scope with pre-existing chain
// history admits at P). The pending nonce is never reused, and the pending
// evidence is never merged into the sequence. This pins existing policy; it
// introduces no new admission rule.
func TestNonceBootstrapPendingOnlySpikeAdmitsAtP(t *testing.T) {
	ctx := context.Background()
	pool := bootMigratedPool(t)

	anvil := bootStartAnvilNode(t)
	// accounts[1] is a fresh sender: its nonce starts at 0 on a clean node.
	sender := strings.ToLower(anvil.accounts[1].Hex())
	anvil.bootDisableAutomine(t)
	bootSeedScope(t, ctx, pool, sender)

	burn := common.HexToAddress("0x000000000000000000000000000000000000dead")
	anvil.sendPendingTx(t, anvil.accounts[1], burn)
	bootAssertCounts(t, anvil, sender, 0, 1)

	observer := nonce.NewObserver(anvil.rpc, nonce.ObserverConfig{
		RPCTimeout:   5 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	alloc := nonce.NewAllocator(pool, observer, admitGate)
	req := nonce.AllocationRequest{
		IntentID:        "boot-intent-pending-spike",
		ChainID:         bootChainID,
		Sender:          sender,
		AuthorizationID: bootAuthorization,
	}

	binding, outcome, err := alloc.Allocate(ctx, req)
	if err != nil || outcome != nonce.OutcomeAllocated || binding == nil {
		t.Fatalf("pending-only bootstrap allocate = (%+v, %q, %v), want allocated", binding, outcome, err)
	}

	// Admit at P: the queued nonce is never handed out again.
	p := big.NewInt(1)
	if binding.Nonce.Cmp(p) != 0 {
		t.Fatalf("admitted nonce = %s, want P = %s", binding.Nonce, p)
	}

	obs := bootReadObservation(t, ctx, pool, binding.AllocationObservationID)
	if obs.classification != "bootstrap_external_consumed" {
		t.Fatalf("observation classification = %q, want bootstrap_external_consumed", obs.classification)
	}
	if obs.latest != "0" || obs.pending != p.String() {
		t.Fatalf("observation view = latest %q pending %q, want 0/%s", obs.latest, obs.pending, p)
	}

	if n := bootCount(t, ctx, pool,
		`SELECT count(*) FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2`,
		bootChainID, sender); n != 0 {
		t.Fatalf("pending-only bootstrap created %d hold(s), want 0", n)
	}
	if n := bootCount(t, ctx, pool,
		`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2 AND nonce < $3`,
		bootChainID, sender, p.String()); n != 0 {
		t.Fatalf("%d binding(s) below P — the pending spike was silently merged", n)
	}
}
