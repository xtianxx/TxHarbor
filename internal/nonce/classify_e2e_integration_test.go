//go:build integration

// classify_e2e_integration_test.go owns spec task T027 for 008-nonce-manager:
// the V5 classification/holds E2E over a real PostgreSQL plus a real Anvil
// (the `logscanStartAnvilNode` precedent: foundry v1.8.1, chain 31337, only
// container-mapped loopback ports).
//
// Anvil technique per scenario (recorded as T027 requires):
//   - unattributed_consumption: two REAL transfers from one Anvil default
//     account mined with automine on, so the durable frontier M=0 yields
//     M+1=1 while the view is L=2,P=2 (L > M+1);
//   - unexplained_gap: `evm_setAutomine false` plus two queued transfers at
//     explicit nonces, so P=2 > M+1=1 while L=0 <= M+1;
//   - unavailable: the observer is dialed to a CLOSED loopback port (no Anvil
//     view at all);
//   - chain_view_divergence: the durable nonce_scope_state.last_pending is
//     seeded above the live pending (a stored-view regression; the two-
//     observer/differing-head technique was not needed).
//
// The file is package nonce_test: admission is driven through the exported
// nonce.NewAllocator / Allocate and every classification/hold/evidence
// assertion is a raw-SQL probe (the allocate_concurrency_integration_test.go
// convention). No Serve process, no adoption of the chain view, and no
// silent reuse of a refused nonce anywhere.
package nonce_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	// clsChainID is the Anvil container's chain id (--chain-id 31337).
	clsChainID = int64(31337)
	// clsAuthPrefix keeps each scenario's 007 authorization id distinct.
	clsAuthPrefix = "wa-classify-e2e"
)

// clsSink is the transfer destination every Anvil transaction targets.
var clsSink = common.HexToAddress("0x000000000000000000000000000000000000dead")

// clsAddr returns a lowercase 0x + 40 hex address filled with one byte.
func clsAddr(fill string) string { return "0x" + strings.Repeat(fill, 20) }

// clsLower renders an Anvil account as the lowercase scope address the
// observer's validation accepts (common.Address.Hex is EIP-55 checksummed).
func clsLower(a common.Address) string { return strings.ToLower(a.Hex()) }

// --- PostgreSQL harness -----------------------------------------------------

// clsSetup boots one isolated PostgreSQL container, migrates it to 000008 and
// opens the pooled handle the allocator drives. It reuses the T005 helpers
// (nonceStartPostgres/nonceMigrateOptions) exactly as T018/T019 do.
func clsSetup(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	dsn := nonceStartPostgres(t)
	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return ctx, pool
}

// clsMustExec fails the test on any raw exec error.
func clsMustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// clsSeedScope makes one Anvil default account admissible: the scrubbed 007
// caller, the active 008 registry row, and one active 007 authorization.
func clsSeedScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender, authID string) {
	t.Helper()
	clsMustExec(t, ctx, pool,
		`INSERT INTO caller (caller_id, label) VALUES (1, 'cls-e2e')
		 ON CONFLICT (caller_id) DO NOTHING`)
	clsMustExec(t, ctx, pool,
		`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		 VALUES ($1, $2, 'active', 1)`, clsChainID, sender)
	clsMustExec(t, ctx, pool,
		`INSERT INTO withdrawal_authorizations
		 (authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		 VALUES ($1, 1, $2, $3, $4, 1, 'active')`, authID, clsChainID, clsAddr("11"), clsAddr("22"))
}

// --- Anvil harness ----------------------------------------------------------

// clsAnvilNode is the real chain truth: a pinned Anvil container plus its
// JSON-RPC client and unlocked default accounts.
type clsAnvilNode struct {
	client   *gethrpc.Client
	accounts []common.Address
}

// clsStartAnvil boots the pinned Anvil image (v1.8.1, chain 31337). It skips
// (never passes) without a healthy Docker provider.
func clsStartAnvil(t *testing.T) *clsAnvilNode {
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
	client, err := gethrpc.DialContext(ctx, fmt.Sprintf("http://%s:%s", host, port.Port()))
	if err != nil {
		t.Fatalf("dial anvil rpc: %v", err)
	}
	t.Cleanup(client.Close)

	var accounts []common.Address
	if err := client.CallContext(ctx, &accounts, "eth_accounts"); err != nil {
		t.Fatalf("eth_accounts: %v", err)
	}
	if len(accounts) == 0 {
		t.Fatal("anvil returned no unlocked accounts")
	}
	return &clsAnvilNode{client: client, accounts: accounts}
}

func (n *clsAnvilNode) mustCall(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.client.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

// setAutomine flips Anvil's automine; with it off a broadcast tx is queued.
func (n *clsAnvilNode) setAutomine(t *testing.T, on bool) {
	t.Helper()
	var ok bool
	n.mustCall(t, &ok, "evm_setAutomine", on)
}

// sendTxAt broadcasts one zero-value transfer from `from` at an explicit
// nonce. With automine on it mines; with automine off it stays pending.
func (n *clsAnvilNode) sendTxAt(t *testing.T, from common.Address, nonce uint64) {
	t.Helper()
	var hash common.Hash
	n.mustCall(t, &hash, "eth_sendTransaction", map[string]any{
		"from":  from,
		"to":    clsSink,
		"gas":   "0x5208",
		"nonce": hexutil.EncodeUint64(nonce),
	})
}

// waitCounts polls the live chain view until latest/pending match exactly.
func (n *clsAnvilNode) waitCounts(t *testing.T, sender string, latest, pending uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var l, p hexutil.Uint64
		n.mustCall(t, &l, "eth_getTransactionCount", sender, "latest")
		n.mustCall(t, &p, "eth_getTransactionCount", sender, "pending")
		if uint64(l) == latest && uint64(p) == pending {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("chain view latest=%d pending=%d, want %d/%d", uint64(l), uint64(p), latest, pending)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// clsClosedPortRPC dials a loopback port that was just released, so every call
// fails with a transport error — a real RPC outage without any fixture.
func clsClosedPortRPC(t *testing.T) *gethrpc.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved loopback port: %v", err)
	}
	client, err := gethrpc.DialContext(context.Background(), "http://"+addr)
	if err != nil {
		t.Fatalf("dial closed loopback port: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// --- allocation + raw-SQL probes -------------------------------------------

// clsAllocator wires the exported admission core over the client.
func clsAllocator(t *testing.T, client *gethrpc.Client, pool *pgxpool.Pool) *nonce.Allocator {
	t.Helper()
	observer := nonce.NewObserver(client, nonce.ObserverConfig{
		RPCTimeout:   5 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	admitGate := nonce.NewRebuildGate()
	admitGate.Open()
	return nonce.NewAllocator(pool, observer, admitGate)
}

func clsRequest(intentID, sender, authID string) nonce.AllocationRequest {
	return nonce.AllocationRequest{
		IntentID:        intentID,
		ChainID:         clsChainID,
		Sender:          sender,
		AuthorizationID: authID,
	}
}

func clsIntentBindings(t *testing.T, ctx context.Context, pool *pgxpool.Pool, intentID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, intentID).Scan(&n); err != nil {
		t.Fatalf("count bindings for intent %s: %v", intentID, err)
	}
	return n
}

func clsScopeBindings(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2`, clsChainID, sender).Scan(&n); err != nil {
		t.Fatalf("count scope bindings: %v", err)
	}
	return n
}

func clsOnlyBindingNonce(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) string {
	t.Helper()
	var nonceText string
	if err := pool.QueryRow(ctx,
		`SELECT nonce::text FROM nonce_bindings WHERE chain_id = $1 AND sender = $2`, clsChainID, sender).Scan(&nonceText); err != nil {
		t.Fatalf("read the scope's single binding: %v", err)
	}
	return nonceText
}

func clsObservationCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_observations WHERE chain_id = $1 AND sender = $2`, clsChainID, sender).Scan(&n); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	return n
}

// clsLastObservation returns the scope's newest persisted evidence row.
func clsLastObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) (class, errorClass string) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT classification, error_class FROM nonce_observations
		WHERE chain_id = $1 AND sender = $2 ORDER BY observed_at DESC, observation_id DESC`, clsChainID, sender)
	if err != nil {
		t.Fatalf("query observations: %v", err)
	}
	defer rows.Close()
	found := 0
	for rows.Next() {
		if err := rows.Scan(&class, &errorClass); err != nil {
			t.Fatalf("scan observation: %v", err)
		}
		found++
		break
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate observations: %v", err)
	}
	if found != 1 {
		t.Fatalf("no observation persisted for %s", sender)
	}
	return class, errorClass
}

func clsHoldCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_scope_holds WHERE chain_id = $1 AND sender = $2 AND status = 'active'`,
		clsChainID, sender).Scan(&n); err != nil {
		t.Fatalf("count active holds: %v", err)
	}
	return n
}

// clsActiveHold returns the single active hold of the given cause joined to
// its persisted evidence observation (classification + the observed view).
func clsActiveHold(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender, cause string) (class, latest, pending, detail string) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT o.classification, o.latest_count::text, o.pending_count::text, h.evidence_detail
		FROM nonce_scope_holds h
		JOIN nonce_observations o ON o.observation_id = h.evidence_observation_id
		WHERE h.chain_id = $1 AND h.sender = $2 AND h.status = 'active' AND h.cause = $3`,
		clsChainID, sender, cause)
	if err != nil {
		t.Fatalf("query active hold %q: %v", cause, err)
	}
	defer rows.Close()
	found := 0
	for rows.Next() {
		if err := rows.Scan(&class, &latest, &pending, &detail); err != nil {
			t.Fatalf("scan active hold: %v", err)
		}
		found++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate active holds: %v", err)
	}
	if found != 1 {
		t.Fatalf("active %q holds for %s = %d, want exactly 1", cause, sender, found)
	}
	return class, latest, pending, detail
}

// clsScopeFrontier reads the durable scope row's frontier columns (nil = SQL
// NULL), proving an unavailable read changed no durable state.
func clsScopeFrontier(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) (floor, latest, pending *string) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		`SELECT reconciled_floor::text, last_latest::text, last_pending::text
		 FROM nonce_scope_state WHERE chain_id = $1 AND sender = $2`,
		clsChainID, sender).Scan(&floor, &latest, &pending); err != nil {
		t.Fatalf("read scope frontier: %v", err)
	}
	return floor, latest, pending
}

// --- T027 -------------------------------------------------------------------

// TestNonceClassificationHoldsE2E is the T027 V5 acceptance over real
// PostgreSQL + real Anvil: each classification above a durable frontier holds
// and refuses, an unavailable read changes no durable state, and every
// classification, hold and evidence row is queryable with zero silent
// reuse/adoption of the chain view.
func TestNonceClassificationHoldsE2E(t *testing.T) {
	ctx, pool := clsSetup(t)
	anvil := clsStartAnvil(t)
	if len(anvil.accounts) < 4 {
		t.Fatalf("anvil returned %d default accounts, want at least 4 (one per scenario)", len(anvil.accounts))
	}

	// External mined consumption above the frontier: L > M+1.
	t.Run("external_mined_consumption_above_frontier", func(t *testing.T) {
		sender := clsLower(anvil.accounts[0])
		authID := clsAuthPrefix + "-unattributed"
		clsSeedScope(t, ctx, pool, sender, authID)
		alloc := clsAllocator(t, anvil.client, pool)

		first, outcome, err := alloc.Allocate(ctx, clsRequest("cls-A1", sender, authID))
		if err != nil || outcome != nonce.OutcomeAllocated || first == nil {
			t.Fatalf("first allocate = (%v, %q, %v), want allocated", first, outcome, err)
		}
		if first.Nonce.Uint64() != 0 {
			t.Fatalf("first nonce = %s, want 0", first.Nonce)
		}

		// Two real mined transfers: M=0 so M+1=1, but L=P=2 (L > M+1).
		anvil.sendTxAt(t, anvil.accounts[0], 0)
		anvil.sendTxAt(t, anvil.accounts[0], 1)
		anvil.waitCounts(t, sender, 2, 2)

		refused, outcome, err := alloc.Allocate(ctx, clsRequest("cls-A2", sender, authID))
		if refused != nil {
			t.Fatalf("refused allocation adopted a binding: %+v", refused)
		}
		if outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
			t.Fatalf("second allocate = (%q, %v), want scope_held", outcome, err)
		}
		if !strings.Contains(err.Error(), nonce.CauseUnattributedConsumption) {
			t.Fatalf("refusal reason %q does not name %q", err.Error(), nonce.CauseUnattributedConsumption)
		}

		// Classification + hold + evidence are queryable through the join.
		class, latest, pending, detail := clsActiveHold(t, ctx, pool, sender, nonce.CauseUnattributedConsumption)
		if class != nonce.ClassificationUnattributedConsumption || latest != "2" || pending != "2" {
			t.Fatalf("hold evidence = class %q L %q P %q, want unattributed_consumption 2/2", class, latest, pending)
		}
		if detail != nonce.ClassificationUnattributedConsumption {
			t.Fatalf("hold evidence_detail = %q, want %q", detail, nonce.ClassificationUnattributedConsumption)
		}

		// Zero silent reuse/adoption: no binding for the refused intent, the
		// original binding keeps nonce 0, and a later intent stays refused.
		if n := clsIntentBindings(t, ctx, pool, "cls-A2"); n != 0 {
			t.Fatalf("refused intent cls-A2 has %d bindings, want 0", n)
		}
		if n := clsScopeBindings(t, ctx, pool, sender); n != 1 {
			t.Fatalf("scope bindings = %d, want 1", n)
		}
		if got := clsOnlyBindingNonce(t, ctx, pool, sender); got != "0" {
			t.Fatalf("original binding nonce = %q, want 0 (never reassigned)", got)
		}
		obsBefore := clsObservationCount(t, ctx, pool, sender)
		late, lateOutcome, lateErr := alloc.Allocate(ctx, clsRequest("cls-A3", sender, authID))
		if late != nil || lateOutcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(lateErr, nonce.OutcomeScopeHeld) {
			t.Fatalf("held-scope allocate = (%v, %q, %v), want scope_held with no binding", late, lateOutcome, lateErr)
		}
		if n := clsObservationCount(t, ctx, pool, sender); n != obsBefore {
			t.Fatalf("held recheck persisted an observation: %d -> %d", obsBefore, n)
		}
		if n := clsIntentBindings(t, ctx, pool, "cls-A3"); n != 0 {
			t.Fatalf("held-scope intent cls-A3 has %d bindings, want 0", n)
		}
	})

	// Pending-only above the frontier: P > M+1 with L <= M+1.
	t.Run("pending_only_above_frontier", func(t *testing.T) {
		sender := clsLower(anvil.accounts[1])
		authID := clsAuthPrefix + "-unexplained"
		clsSeedScope(t, ctx, pool, sender, authID)
		alloc := clsAllocator(t, anvil.client, pool)

		first, outcome, err := alloc.Allocate(ctx, clsRequest("cls-B1", sender, authID))
		if err != nil || outcome != nonce.OutcomeAllocated || first == nil {
			t.Fatalf("first allocate = (%v, %q, %v), want allocated", first, outcome, err)
		}
		if first.Nonce.Uint64() != 0 {
			t.Fatalf("first nonce = %s, want 0", first.Nonce)
		}

		// Queued-only window: automine off, two transfers at explicit nonces,
		// so P=2 > M+1=1 with L=0 <= M+1.
		anvil.setAutomine(t, false)
		defer anvil.setAutomine(t, true)
		anvil.sendTxAt(t, anvil.accounts[1], 0)
		anvil.sendTxAt(t, anvil.accounts[1], 1)
		anvil.waitCounts(t, sender, 0, 2)

		refused, outcome, err := alloc.Allocate(ctx, clsRequest("cls-B2", sender, authID))
		if refused != nil {
			t.Fatalf("refused allocation adopted a binding: %+v", refused)
		}
		if outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
			t.Fatalf("second allocate = (%q, %v), want scope_held", outcome, err)
		}
		if !strings.Contains(err.Error(), nonce.CauseUnexplainedGap) {
			t.Fatalf("refusal reason %q does not name %q", err.Error(), nonce.CauseUnexplainedGap)
		}

		class, latest, pending, detail := clsActiveHold(t, ctx, pool, sender, nonce.CauseUnexplainedGap)
		if class != nonce.ClassificationUnexplainedGap || latest != "0" || pending != "2" {
			t.Fatalf("hold evidence = class %q L %q P %q, want unexplained_gap 0/2", class, latest, pending)
		}
		if detail != nonce.ClassificationUnexplainedGap {
			t.Fatalf("hold evidence_detail = %q, want %q", detail, nonce.ClassificationUnexplainedGap)
		}

		if n := clsIntentBindings(t, ctx, pool, "cls-B2"); n != 0 {
			t.Fatalf("refused intent cls-B2 has %d bindings, want 0", n)
		}
		if n := clsScopeBindings(t, ctx, pool, sender); n != 1 {
			t.Fatalf("scope bindings = %d, want 1", n)
		}
		if got := clsOnlyBindingNonce(t, ctx, pool, sender); got != "0" {
			t.Fatalf("original binding nonce = %q, want 0 (never reassigned)", got)
		}
		late, lateOutcome, lateErr := alloc.Allocate(ctx, clsRequest("cls-B3", sender, authID))
		if late != nil || lateOutcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(lateErr, nonce.OutcomeScopeHeld) {
			t.Fatalf("held-scope allocate = (%v, %q, %v), want scope_held with no binding", late, lateOutcome, lateErr)
		}
		if n := clsIntentBindings(t, ctx, pool, "cls-B3"); n != 0 {
			t.Fatalf("held-scope intent cls-B3 has %d bindings, want 0", n)
		}
	})

	// RPC outage: unavailable, no durable state change.
	t.Run("rpc_outage_is_unavailable_without_state_change", func(t *testing.T) {
		sender := clsLower(anvil.accounts[2])
		authID := clsAuthPrefix + "-unavailable"
		clsSeedScope(t, ctx, pool, sender, authID)
		alloc := clsAllocator(t, clsClosedPortRPC(t), pool)

		refused, outcome, err := alloc.Allocate(ctx, clsRequest("cls-C1", sender, authID))
		if refused != nil {
			t.Fatalf("outage allocation adopted a binding: %+v", refused)
		}
		if outcome != nonce.OutcomeChainViewUnavailable || !nonce.IsOutcome(err, nonce.OutcomeChainViewUnavailable) {
			t.Fatalf("outage allocate = (%q, %v), want chain_view_unavailable", outcome, err)
		}

		// The evidence row is queryable and classified unavailable.
		if n := clsObservationCount(t, ctx, pool, sender); n != 1 {
			t.Fatalf("outage observations = %d, want exactly 1", n)
		}
		class, errorClass := clsLastObservation(t, ctx, pool, sender)
		if class != nonce.ClassificationUnavailable {
			t.Fatalf("outage observation classification = %q, want unavailable", class)
		}
		switch errorClass {
		case "transport", "timeout", "rpc_unavailable":
		default:
			t.Fatalf("outage error_class = %q, want a transport-class failure", errorClass)
		}

		// No durable state changed: no binding, no hold, no frontier facts.
		if n := clsScopeBindings(t, ctx, pool, sender); n != 0 {
			t.Fatalf("outage created %d bindings, want 0", n)
		}
		if n := clsHoldCount(t, ctx, pool, sender); n != 0 {
			t.Fatalf("outage created %d active holds, want 0", n)
		}
		floor, latest, pending := clsScopeFrontier(t, ctx, pool, sender)
		if floor != nil || latest != nil || pending != nil {
			t.Fatalf("outage moved the scope frontier: floor=%v latest=%v pending=%v, want all NULL", floor, latest, pending)
		}
	})

	// Divergent/regressing view: a stored last_pending above the live pending.
	t.Run("stored_pending_regression_is_divergence", func(t *testing.T) {
		sender := clsLower(anvil.accounts[3])
		authID := clsAuthPrefix + "-divergence"
		clsSeedScope(t, ctx, pool, sender, authID)

		// TECHNIQUE: seed the durable frontier's last_pending above the live
		// pending, so the fresh observation (P=0) regresses below it.
		clsMustExec(t, ctx, pool,
			`INSERT INTO nonce_scope_state (chain_id, sender, last_latest, last_pending)
			 VALUES ($1, $2, 0, 5)`, clsChainID, sender)

		alloc := clsAllocator(t, anvil.client, pool)
		refused, outcome, err := alloc.Allocate(ctx, clsRequest("cls-D1", sender, authID))
		if refused != nil {
			t.Fatalf("divergence allocation adopted a binding: %+v", refused)
		}
		if outcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(err, nonce.OutcomeScopeHeld) {
			t.Fatalf("divergence allocate = (%q, %v), want scope_held", outcome, err)
		}
		if !strings.Contains(err.Error(), nonce.ClassificationDivergence) {
			t.Fatalf("refusal reason %q does not name %q", err.Error(), nonce.ClassificationDivergence)
		}

		class, latest, pending, detail := clsActiveHold(t, ctx, pool, sender, nonce.CauseChainViewDivergence)
		if class != nonce.ClassificationDivergence || latest != "0" || pending != "0" {
			t.Fatalf("hold evidence = class %q L %q P %q, want divergence 0/0", class, latest, pending)
		}
		if detail != nonce.ClassificationDivergence {
			t.Fatalf("hold evidence_detail = %q, want %q", detail, nonce.ClassificationDivergence)
		}

		// Zero adoption: the contradictory view never becomes a candidate, and
		// a later intent on the held scope is refused with no binding.
		if n := clsIntentBindings(t, ctx, pool, "cls-D1"); n != 0 {
			t.Fatalf("refused intent cls-D1 has %d bindings, want 0", n)
		}
		if n := clsScopeBindings(t, ctx, pool, sender); n != 0 {
			t.Fatalf("divergent scope bindings = %d, want 0", n)
		}
		late, lateOutcome, lateErr := alloc.Allocate(ctx, clsRequest("cls-D2", sender, authID))
		if late != nil || lateOutcome != nonce.OutcomeScopeHeld || !nonce.IsOutcome(lateErr, nonce.OutcomeScopeHeld) {
			t.Fatalf("held-scope allocate = (%v, %q, %v), want scope_held with no binding", late, lateOutcome, lateErr)
		}
		if n := clsIntentBindings(t, ctx, pool, "cls-D2"); n != 0 {
			t.Fatalf("held-scope intent cls-D2 has %d bindings, want 0", n)
		}
	})
}
