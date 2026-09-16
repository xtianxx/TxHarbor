//go:build integration

// unknown_outcome_integration_test.go owns spec task T024 for 008-nonce-manager:
// the V4 unknown-outcome retention E2E over a real PostgreSQL plus a real Anvil
// (the `logscanStartAnvilNode` precedent: foundry v1.8.1, chain 31337).
//
// Route used: the live-pending route (the durable-row fallback was NOT needed).
// Anvil automine is switched off with `evm_setAutomine false` and a transaction
// is broadcast from an Anvil default account at exactly the bound nonce, so the
// chain itself reports the bound nonce as pending (`latest = N`,
// `pending = N+1`) — a real `nonce in [L,P)` view, not a seeded observation.
//
// The file is package nonce (not nonce_test) because the T-observe carriers
// (`lockChain`, `readBindingsByScopeTx`, `insertObservationTx`,
// `applyObservationTransitionTx`) are package-private; the test drives them
// directly, exactly as the reconcile_integration_test.go sibling (T026) does.
//
// Assertions: the bound nonce in the pending window transitions
// `allocated -> in_flight` with exactly one event linked to the persisted
// observation; the binding keeps its original intent and is never
// auto-failed/recycled/reassigned; every observation row is retained
// (100 % evidence); a same-intent replacement replays the SAME `binding_id`
// and a different intent is never handed the pending/bound nonce.
package nonce

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
)

const (
	unknownAuth = "wa-unknown-retention"
	// unknownChainID is the Anvil container's chain id (--chain-id 31337).
	unknownChainID = int64(31337)
)

// unknownAnvilNode is the real chain truth: a pinned Anvil container plus the
// JSON-RPC client and the unlocked default accounts.
type unknownAnvilNode struct {
	url      string
	rpc      *rpc.Client
	accounts []common.Address
}

// unknownStartAnvilNode boots the pinned Anvil image (v1.8.1, chain 31337).
// Skips (never passes) without a healthy Docker provider.
func unknownStartAnvilNode(t *testing.T) *unknownAnvilNode {
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
	return &unknownAnvilNode{url: fmt.Sprintf("http://%s:%s", host, port.Port()), rpc: c, accounts: accounts}
}

func (n *unknownAnvilNode) mustCall(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.rpc.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

// setAutomine flips Anvil's automine; with it off a broadcast tx stays pending.
func (n *unknownAnvilNode) setAutomine(t *testing.T, on bool) {
	t.Helper()
	var ok bool
	n.mustCall(t, &ok, "evm_setAutomine", on)
}

// sendPendingTx broadcasts a zero-value transfer from the first default account
// at an explicit nonce (automine must be off, so it is queued, not mined).
func (n *unknownAnvilNode) sendPendingTx(t *testing.T, to common.Address, nonce uint64) common.Hash {
	t.Helper()
	var txHash common.Hash
	n.mustCall(t, &txHash, "eth_sendTransaction", map[string]any{
		"from":  n.accounts[0],
		"to":    to,
		"gas":   "0x5208",
		"nonce": hexutil.EncodeUint64(nonce),
	})
	return txHash
}

func (n *unknownAnvilNode) mine(t *testing.T, blocks uint64) {
	t.Helper()
	var res any
	n.mustCall(t, &res, "anvil_mine", hexutil.EncodeUint64(blocks))
}

// unknownAssertCounts pins the exact chain view the observer must see.
func unknownAssertCounts(t *testing.T, n *unknownAnvilNode, sender string, latest, pending uint64) {
	t.Helper()
	var gotLatest, gotPending hexutil.Uint64
	n.mustCall(t, &gotLatest, "eth_getTransactionCount", sender, "latest")
	n.mustCall(t, &gotPending, "eth_getTransactionCount", sender, "pending")
	if uint64(gotLatest) != latest || uint64(gotPending) != pending {
		t.Fatalf("chain view latest=%d pending=%d, want %d/%d", uint64(gotLatest), uint64(gotPending), latest, pending)
	}
}

// unknownSeedScope registers the Anvil default account and supplies one active
// 007 authorization so admission reaches the binding insert.
func unknownSeedScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, sender string) {
	t.Helper()
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatalf("exec %q: %v", query, err)
		}
	}
	mustExec(`INSERT INTO caller (caller_id, label, can_create) VALUES (1, 'unknown-it', TRUE)`)
	mustExec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES ($1, $2, 'active', 1)`, chainID, sender)
	mustExec(`INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, unknownAuth, chainID, sender, sender)
}

// unknownReconcileScope runs one full T-observe tick with the production
// carriers: pre-tx observation, coordination + scope locks, classify, persist
// the evidence observation, apply the automatic transition per binding, update
// the scope frontier, commit. It returns the persisted observation.
func unknownReconcileScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, observer *Observer, chainID int64, sender string) Observation {
	t.Helper()
	obs := observer.Observe(ctx, chainID, sender, ObservationKindReconcile)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reconcile tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := lockChain(ctx, tx, chainID); err != nil {
		t.Fatalf("reconcile coordination lock: %v", err)
	}
	if err := ensureScopeRowTx(ctx, tx, chainID, sender); err != nil {
		t.Fatalf("reconcile ensure scope row: %v", err)
	}
	if err := lockScopeRowTx(ctx, tx, chainID, sender); err != nil {
		t.Fatalf("reconcile scope lock: %v", err)
	}
	bindings, err := readBindingsByScopeTx(ctx, tx, chainID, sender)
	if err != nil {
		t.Fatalf("reconcile read bindings: %v", err)
	}
	scope, err := readScopeStateTx(ctx, tx, chainID, sender)
	if err != nil {
		t.Fatalf("reconcile read scope: %v", err)
	}

	decision := Classify(ClassificationInput{
		ReadFailed:      obs.Classification == ClassificationUnavailable,
		Latest:          obs.LatestCount,
		Pending:         obs.PendingCount,
		PendingPrev:     scopeLastPending(scope),
		MaxBindingNonce: MaxBoundNonce(bindings),
		HasBindings:     len(bindings) > 0,
		ReconciledFloor: scopeFloor(scope),
	})
	obs.Classification = decision.Classification
	obs.ObservationID, err = insertObservationTx(ctx, tx, obs)
	if err != nil {
		t.Fatalf("reconcile persist observation: %v", err)
	}
	for i := range bindings {
		if _, err := applyObservationTransitionTx(ctx, tx, bindings[i], obs); err != nil {
			t.Fatalf("reconcile transition %s: %v", bindings[i].BindingID, err)
		}
	}
	if obs.LatestCount != nil && obs.PendingCount != nil {
		if err := updateScopeFrontierTx(ctx, tx, chainID, sender, obs.LatestCount, obs.PendingCount, obs.ObservationID); err != nil {
			t.Fatalf("reconcile update frontier: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("reconcile commit: %v", err)
	}
	return obs
}

// unknownBindingRow reads the durable binding facts under test.
func unknownBindingRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID string) (state, intentID, nonceText string) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		`SELECT state, intent_id, nonce::text FROM nonce_bindings WHERE binding_id = $1`, bindingID).
		Scan(&state, &intentID, &nonceText); err != nil {
		t.Fatalf("read binding %s: %v", bindingID, err)
	}
	return state, intentID, nonceText
}

// unknownAssertSingleTransition proves exactly one event to the target state,
// from the expected predecessor, linked to the persisted observation.
func unknownAssertSingleTransition(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID, to, observationID string) {
	t.Helper()
	var (
		count int
		from  *string
		obs   *string
	)
	if err := pool.QueryRow(ctx,
		`SELECT count(*), min(from_state)::text, min(observation_id)::text
		 FROM nonce_binding_events WHERE binding_id = $1 AND to_state = $2`,
		bindingID, to).Scan(&count, &from, &obs); err != nil {
		t.Fatalf("read %s event for %s: %v", to, bindingID, err)
	}
	if count != 1 {
		t.Fatalf("%s events for %s = %d, want exactly 1", to, bindingID, count)
	}
	if from == nil || *from != StateAllocated {
		t.Fatalf("%s event from_state = %v, want %q", to, from, StateAllocated)
	}
	if obs == nil || *obs != observationID {
		t.Fatalf("%s event observation_id = %v, want the persisted %q", to, obs, observationID)
	}
}

// unknownAssertNeverReleased proves no release path ran for the binding.
func unknownAssertNeverReleased(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID string) {
	t.Helper()
	var releasedEvents int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_binding_events WHERE binding_id = $1 AND to_state = 'released'`, bindingID).
		Scan(&releasedEvents); err != nil {
		t.Fatalf("count released events for %s: %v", bindingID, err)
	}
	if releasedEvents != 0 {
		t.Fatalf("binding %s gained %d released events, want 0", bindingID, releasedEvents)
	}
	if state, _, _ := unknownBindingRow(t, ctx, pool, bindingID); state == StateReleased {
		t.Fatalf("binding %s is released; an unknown outcome is never auto-failed", bindingID)
	}
}

// unknownObservationRow reads one retained evidence row.
func unknownObservationRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, observationID string) (kind, class, latest, pending string) {
	t.Helper()
	if err := pool.QueryRow(ctx,
		`SELECT kind, classification, latest_count::text, pending_count::text
		 FROM nonce_observations WHERE observation_id = $1`, observationID).
		Scan(&kind, &class, &latest, &pending); err != nil {
		t.Fatalf("read observation %s: %v", observationID, err)
	}
	return kind, class, latest, pending
}

func unknownObservationExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, observationID string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_observations WHERE observation_id = $1`, observationID).Scan(&n); err != nil {
		t.Fatalf("count observation %s: %v", observationID, err)
	}
	return n == 1
}

func unknownObservationCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, sender string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_observations WHERE chain_id = $1 AND sender = $2`, chainID, sender).Scan(&n); err != nil {
		t.Fatalf("count observations: %v", err)
	}
	return n
}

func unknownBindingCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID int64, sender string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM nonce_bindings WHERE chain_id = $1 AND sender = $2`, chainID, sender).Scan(&n); err != nil {
		t.Fatalf("count bindings: %v", err)
	}
	return n
}

// TestNonceUnknownOutcomeRetentionE2E is the T024 V4 acceptance: a real pending
// bound nonce moves the binding to in_flight with evidence, and no path ever
// recycles, reassigns or auto-fails it.
func TestNonceUnknownOutcomeRetentionE2E(t *testing.T) {
	ctx, pool := convergeSetup(t)

	anvil := unknownStartAnvilNode(t)
	sender := strings.ToLower(anvil.accounts[0].Hex())
	unknownSeedScope(t, ctx, pool, unknownChainID, sender)

	observer := NewObserver(anvil.rpc, ObserverConfig{
		RPCTimeout:   5 * time.Second,
		RetryInitial: time.Millisecond,
		RetryMax:     5 * time.Millisecond,
	})
	alloc := NewAllocator(pool, observer)

	// --- 1) admit the original intent while the account nonce is clean -----
	original := AllocationRequest{
		IntentID:        "unknown-intent-original",
		ChainID:         unknownChainID,
		Sender:          sender,
		AuthorizationID: unknownAuth,
	}
	binding, outcome, err := alloc.Allocate(ctx, original)
	if err != nil || outcome != OutcomeAllocated || binding == nil {
		t.Fatalf("original allocate = (%+v, %q, %v), want a new binding", binding, outcome, err)
	}
	boundNonce := new(big.Int).Set(binding.Nonce)
	if boundNonce.Sign() != 0 {
		t.Fatalf("bound nonce = %s, want 0 on a fresh Anvil account", boundNonce)
	}
	if got := unknownObservationCount(t, ctx, pool, unknownChainID, sender); got != 1 {
		t.Fatalf("observations after admission = %d, want 1", got)
	}

	// --- 2) broadcast the bound nonce; keep it pending (automine off) ------
	anvil.setAutomine(t, false)
	anvil.sendPendingTx(t, common.HexToAddress("0x000000000000000000000000000000000000dead"), boundNonce.Uint64())
	unknownAssertCounts(t, anvil, sender, boundNonce.Uint64(), boundNonce.Uint64()+1)

	// --- 3) T-observe: allocated -> in_flight with retained evidence -------
	reconcileObs := unknownReconcileScope(t, ctx, pool, observer, unknownChainID, sender)
	if reconcileObs.Classification != ClassificationConsistent {
		t.Fatalf("pending reconcile classified %q, want %q", reconcileObs.Classification, ClassificationConsistent)
	}
	state, intentID, nonceText := unknownBindingRow(t, ctx, pool, binding.BindingID)
	if state != StateInFlight {
		t.Fatalf("state after pending observation = %q, want %q", state, StateInFlight)
	}
	if intentID != original.IntentID || nonceText != boundNonce.String() {
		t.Fatalf("binding drifted: intent %q nonce %q, want %q/%s", intentID, nonceText, original.IntentID, boundNonce)
	}
	unknownAssertSingleTransition(t, ctx, pool, binding.BindingID, StateInFlight, reconcileObs.ObservationID)

	// Evidence is 100 % retained: the reconcile row still carries the pending
	// window, the admission evidence survives, and no observation was dropped.
	if kind, class, latest, pending := unknownObservationRow(t, ctx, pool, reconcileObs.ObservationID); kind != ObservationKindReconcile ||
		class != ClassificationConsistent || latest != boundNonce.String() || pending != new(big.Int).Add(boundNonce, big.NewInt(1)).String() {
		t.Fatalf("reconcile evidence = kind %q class %q L %q P %q", kind, class, latest, pending)
	}
	if !unknownObservationExists(t, ctx, pool, binding.AllocationObservationID) {
		t.Fatalf("admission observation %q was not retained", binding.AllocationObservationID)
	}
	if got := unknownObservationCount(t, ctx, pool, unknownChainID, sender); got != 2 {
		t.Fatalf("observations after first reconcile = %d, want 2", got)
	}

	// --- 4) the nonce mines; it is still never released/recycled ----------
	anvil.setAutomine(t, true)
	anvil.mine(t, 1)
	_ = unknownReconcileScope(t, ctx, pool, observer, unknownChainID, sender) // mined window re-observed
	state, intentID, nonceText = unknownBindingRow(t, ctx, pool, binding.BindingID)
	if state == StateReleased || state == StateAllocated {
		t.Fatalf("mined outcome state = %q, want retained (never released/recycled)", state)
	}
	if intentID != original.IntentID || nonceText != boundNonce.String() {
		t.Fatalf("binding drifted after mining: intent %q nonce %q", intentID, nonceText)
	}
	unknownAssertNeverReleased(t, ctx, pool, binding.BindingID)
	if got := unknownObservationCount(t, ctx, pool, unknownChainID, sender); got != 3 {
		t.Fatalf("observations after second reconcile = %d, want 3 (all retained)", got)
	}

	// --- 5) a same-intent replacement references the SAME binding ---------
	replacement, routcome, rerr := alloc.Allocate(ctx, original)
	if rerr != nil || routcome != OutcomeReplayed {
		t.Fatalf("same-intent replacement = (%+v, %q, %v), want replayed", replacement, routcome, rerr)
	}
	if replacement == nil || replacement.BindingID != binding.BindingID || replacement.Nonce.Cmp(boundNonce) != 0 {
		t.Fatalf("replacement = %+v, want the original %s/%s", replacement, binding.BindingID, boundNonce)
	}
	if got := unknownObservationCount(t, ctx, pool, unknownChainID, sender); got != 3 {
		t.Fatalf("replay wrote an observation: count %d, want still 3", got)
	}

	// --- 6) another intent is never handed the in-flight nonce ------------
	other := original
	other.IntentID = "unknown-intent-other"
	otherBinding, ooutcome, oerr := alloc.Allocate(ctx, other)
	if oerr != nil || ooutcome != OutcomeAllocated || otherBinding == nil {
		t.Fatalf("other-intent allocate = (%+v, %q, %v), want allocated", otherBinding, ooutcome, oerr)
	}
	if otherBinding.Nonce.Cmp(boundNonce) == 0 {
		t.Fatalf("other intent was assigned the bound nonce %s (reassignment)", boundNonce)
	}
	if want := new(big.Int).Add(boundNonce, big.NewInt(1)); otherBinding.Nonce.Cmp(want) != 0 {
		t.Fatalf("other-intent nonce = %s, want the frontier %s", otherBinding.Nonce, want)
	}
	state, intentID, nonceText = unknownBindingRow(t, ctx, pool, binding.BindingID)
	if state == StateReleased || intentID != original.IntentID || nonceText != boundNonce.String() {
		t.Fatalf("original binding changed after other-intent allocation: state %q intent %q nonce %q", state, intentID, nonceText)
	}
	if got := unknownBindingCount(t, ctx, pool, unknownChainID, sender); got != 2 {
		t.Fatalf("scope bindings = %d, want 2 (original retained + other)", got)
	}
	if got := unknownObservationCount(t, ctx, pool, unknownChainID, sender); got != 4 {
		t.Fatalf("observations at end = %d, want 4 (every attempt retained)", got)
	}
}
