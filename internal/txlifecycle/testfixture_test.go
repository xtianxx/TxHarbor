//go:build integration

package txlifecycle

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
)

// startPG boots a real PostgreSQL container (skips, never passes, without
// Docker) and applies every embedded migration. (T019 scratch DB only.)
func startPG(t *testing.T) string {
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

// stubRPC is the lane-local chain double. It counts dispatch calls so a
// zero-dispatch refusal is observable; it never substitutes for the real
// Anvil/009 acceptance evidence (V-tests that need those use anvilNode).
type stubRPC struct {
	mu         sync.Mutex
	dispatches int
	rawSeen    [][]byte
	expected   common.Hash
	txErr      error
	probeErr   error
	txPending  bool
	txFound    bool
	receipt    *types.Receipt
	head       uint64
}

func (s *stubRPC) SendSignedTransaction(ctx context.Context, raw []byte, expected common.Hash) (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatches++
	s.rawSeen = append(s.rawSeen, append([]byte(nil), raw...))
	s.expected = expected
	if s.txErr != nil {
		return common.Hash{}, s.txErr
	}
	return expected, nil
}

func (s *stubRPC) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.probeErr != nil {
		return nil, false, s.probeErr
	}
	if s.txFound {
		return types.NewTx(&types.LegacyTx{}), s.txPending, nil
	}
	return nil, false, &eth.Error{Kind: eth.KindNotFound, Op: "stub"}
}

func (s *stubRPC) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipt == nil {
		return nil, &eth.Error{Kind: eth.KindNotFound, Op: "stub"}
	}
	return s.receipt, nil
}

func (s *stubRPC) BlockNumber(ctx context.Context) (uint64, error) { return s.head, nil }

func (s *stubRPC) dispatchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dispatches
}

func (s *stubRPC) dispatchedRaw() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rawSeen
}

// env is one migrated scratch database plus its store.
type env struct {
	t       *testing.T
	dsn     string
	pool    *pgxpool.Pool
	store   *Store
	rpc     *stubRPC
	chainID int64
	seq     int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := startPG(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	rpc := &stubRPC{head: 100}
	store := NewStore(pool).WithChain(rpc, 5*time.Second)
	return &env{t: t, dsn: dsn, pool: pool, store: store, rpc: rpc, chainID: 31337}
}

func (e *env) exec(sql string, args ...any) {
	e.t.Helper()
	if _, err := e.pool.Exec(context.Background(), sql, args...); err != nil {
		e.t.Fatalf("exec %q: %v", sql, err)
	}
}

// fixture is the contract-shaped upstream state for one attempt: an 008
// binding, a 007 grant + PB scope, the 006/005 rows, and the test-only J2
// execution_claims row (never a migration, never joint evidence).
type fixture struct {
	env              *env
	key              *ecdsa.PrivateKey
	sender           string
	intentID         string
	bindingID        string
	authID           string
	attemptID        string
	signingRequestID string
	asset            string
	recipient        string
	nonce            string
	workerID         string
	leaseVersion     int64
	request          *PrepareRequest
}

const (
	fxAsset     = "0x2222222222222222222222222222222222222222"
	fxRecipient = "0x3333333333333333333333333333333333333333"
)

// seed writes the whole upstream fixture and persists the attempt (T1).
func (e *env) seed() *fixture {
	e.t.Helper()
	e.seq++
	id := func(prefix string) string { return fmt.Sprintf("%s-%d", prefix, e.seq) }
	ctx := context.Background()
	key, err := crypto.GenerateKey()
	if err != nil {
		e.t.Fatal(err)
	}
	f := &fixture{
		env: e, key: key,
		sender:   strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex()),
		intentID: id("int"), bindingID: id("bind"), authID: id("wa"),
		attemptID: id("att"), signingRequestID: id("sr"),
		asset: fxAsset, recipient: fxRecipient, nonce: "0",
		workerID: id("worker"), leaseVersion: 1,
	}
	if e.seq == 1 {
		e.exec(`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical) VALUES ($1,100,$2,$2,TRUE)`,
			e.chainID, blockHashHex(100))
		e.exec(`INSERT INTO indexer_lease (chain_id, owner_id, expires_at) VALUES ($1,'test',now() + interval '1 hour')`, e.chainID)
		e.exec(`INSERT INTO confirmation_policy_history (chain_id, policy_seq, threshold) VALUES ($1,1,3)`, e.chainID)
		e.exec(`INSERT INTO caller (caller_id, label) VALUES (1,'fixture')`)
	}
	e.exec(`INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1,1,$2,$3,$4,'1000','active')`, f.authID, e.chainID, fxAsset, fxRecipient)
	reqID := id("req")
	e.exec(`INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas, fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1,$2,$3,$4,100000000000000,1000000000,100000000,TRUE,1,'test')`,
		f.authID, f.intentID, reqID, f.sender)
	e.exec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq) VALUES ($1,$2,'active',1)`, e.chainID, f.sender)
	e.exec(`INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1,$2)`, e.chainID, f.sender)
	e.exec(`INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id, authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1,$2,$3,$4,0,'allocated',$5,$6,1,'obs-fixture')`,
		f.bindingID, f.intentID, e.chainID, f.sender, f.authID, strings.Repeat("0", 64))
	e.seedClaimFixtures(f, reqID)

	f.request = &PrepareRequest{
		AttemptID: f.attemptID, SigningRequestID: f.signingRequestID, IntentID: f.intentID,
		BindingRef: f.bindingID, AuthorizationID: f.authID, AuthorizationVersion: 1,
		RecoveryVersion: 0, ChainID: uint64(e.chainID), Sender: f.sender, Nonce: f.nonce,
		TxType: TxTypeDynamicFee, GasLimit: "21000", MaxFeePerGas: "1000000000", MaxPriorityFeePerGas: "100000000",
		Asset: fxAsset, Recipient: fxRecipient, Amount: "1000",
	}
	if _, err := e.store.PrepareAttempt(ctx, f.request); err != nil {
		e.t.Fatalf("PrepareAttempt: %v", err)
	}
	return f
}

// seedClaimFixtures provisions the J2 claim carrier and, when 011's
// payment_intents exists, the intent identity chain the closed intent FK
// needs. On the 011-absent lane a test-only execution_claims table with the
// same column names is created — never a migration, never joint evidence
// (R-010-11; G-010-4; FR-16).
func (e *env) seedClaimFixtures(f *fixture, reqID string) {
	e.exec(`CREATE TABLE IF NOT EXISTS execution_claims (
		intent_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL,
		lease_version BIGINT NOT NULL, state TEXT NOT NULL DEFAULT 'active',
		acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		expires_at TIMESTAMPTZ NOT NULL, ended_at TIMESTAMPTZ, end_kind TEXT)`)
	if e.tableExists("payment_intents") {
		e.exec(`INSERT INTO withdrawal_requests
			(request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
			VALUES ($1,1,$2,$3,$4,$5,$6,'1000')`,
			reqID, "idem-"+reqID, f.authID, e.chainID, fxAsset, fxRecipient)
		e.exec(`INSERT INTO payment_intents
			(intent_id, request_id, chain_id, sender, authorization_id, authorization_version, state, admitted_recovery_version)
			VALUES ($1,$2,$3,$4,$5,1,'admitted',0)`,
			f.intentID, reqID, e.chainID, f.sender, f.authID)
	}
	e.exec(`INSERT INTO execution_claims (intent_id, owner_id, lease_version, state, expires_at)
		VALUES ($1,$2,$3,'active',now() + interval '1 hour')`, f.intentID, f.workerID, f.leaseVersion)
}

func (e *env) tableExists(name string) bool {
	var ok bool
	if err := e.pool.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&ok); err != nil {
		e.t.Fatalf("tableExists %s: %v", name, err)
	}
	return ok
}

func (f *fixture) claim() ClaimRef {
	return ClaimRef{IntentID: f.intentID, WorkerID: f.workerID, LeaseVersion: f.leaseVersion}
}

// sign runs T2 locally with a real signature so the dispatch path exercises
// the persisted bytes (the 009 boundary is covered separately).
func (f *fixture) sign() SignedRecord {
	f.env.t.Helper()
	return f.signRequest(f.request)
}

// signRequest signs an arbitrary persisted attempt with the fixture key.
func (f *fixture) signRequest(req *PrepareRequest) SignedRecord {
	f.env.t.Helper()
	ctx := context.Background()
	unsigned, err := req.Transaction()
	if err != nil {
		f.env.t.Fatal(err)
	}
	signer := types.LatestSignerForChainID(new(big.Int).SetUint64(req.ChainID))
	signed, err := types.SignTx(unsigned, signer, f.key)
	if err != nil {
		f.env.t.Fatal(err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		f.env.t.Fatal(err)
	}
	v, r, s := signed.RawSignatureValues()
	sig := make([]byte, 65)
	r.FillBytes(sig[0:32])
	s.FillBytes(sig[32:64])
	sig[64] = byte(v.Uint64())
	attempt, err := f.env.store.AttemptByID(ctx, req.AttemptID)
	if err != nil {
		f.env.t.Fatal(err)
	}
	rec, err := f.env.store.SignAndPersist(ctx, attempt, SigningResult{
		Signature: "0x" + hex.EncodeToString(sig),
		TxHash:    crypto.Keccak256Hash(raw).Hex(),
	})
	if err != nil {
		f.env.t.Fatalf("SignAndPersist: %v", err)
	}
	return rec
}

func blockHashHex(n uint64) string { return fmt.Sprintf("0x%064x", n) }

// addBlock inserts one chain_blocks row (chain truth for canonicality tests).
func (e *env) addBlock(number uint64, hash string, canonical bool) {
	e.exec(`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical) VALUES ($1,$2,$3,$3,$4)`,
		e.chainID, number, hash, canonical)
}

// xferLog builds one ERC-20 Transfer log for the fixture triple.
func xferLog(asset, sender, recipient string, amount int64) *types.Log {
	return &types.Log{
		Address: common.HexToAddress(asset),
		Topics: []common.Hash{
			eth.TransferSig,
			common.BytesToHash(common.HexToAddress(sender).Bytes()),
			common.BytesToHash(common.HexToAddress(recipient).Bytes()),
		},
		Data: big.NewInt(amount).FillBytes(make([]byte, 32)),
	}
}

// receiptAt builds a receipt at an explicit block/hash.
func receiptAt(number uint64, hash string, status int, logs []*types.Log) *types.Receipt {
	return &types.Receipt{Status: uint64(status), BlockNumber: new(big.Int).SetUint64(number), BlockHash: common.HexToHash(hash), Logs: logs}
}

// reconcileIncluded signs a fresh attempt and drives a reconcile pass that
// observes it included with the supplied receipt.
func (e *env) reconcileIncluded(t *testing.T, f *fixture, receipt *types.Receipt) ReconcileResult {
	t.Helper()
	f.sign()
	e.rpc.mu.Lock()
	e.rpc.txFound = true
	e.rpc.txPending = false
	e.rpc.receipt = receipt
	e.rpc.mu.Unlock()
	t.Cleanup(func() {
		e.rpc.mu.Lock()
		e.rpc.txFound = false
		e.rpc.receipt = nil
		e.rpc.mu.Unlock()
	})
	res, err := e.store.Reconcile(context.Background(), f.attemptID, "")
	if err != nil {
		t.Fatalf("reconcile included: %v", err)
	}
	return res
}

// unknownAttempt drives a dispatch timeout so the attempt lands in unknown
// (business effect undetermined) with bytes/hash intact.
func (e *env) unknownAttempt(t *testing.T) *fixture {
	t.Helper()
	f := e.seed()
	f.sign()
	e.rpc.mu.Lock()
	e.rpc.txErr = &eth.Error{Kind: eth.KindTimeout, Op: "stub"}
	e.rpc.mu.Unlock()
	res, err := e.send(f, SendInitial, nil)
	if err != nil || res.Outcome != "unknown" {
		t.Fatalf("timeout dispatch = %+v %v, want unknown", res, err)
	}
	e.rpc.mu.Lock()
	e.rpc.txErr = nil
	e.rpc.mu.Unlock()
	return f
}

// synthReceipt builds a successful receipt carrying the expected Transfer at
// chain block 100.
func synthReceipt(sender string, block uint64) *types.Receipt {
	return &types.Receipt{
		Status:      1,
		BlockNumber: big.NewInt(int64(block)),
		BlockHash:   common.HexToHash(blockHashHex(block)),
		Logs: []*types.Log{{
			Address: common.HexToAddress(fxAsset),
			Topics: []common.Hash{
				eth.TransferSig,
				common.BytesToHash(common.HexToAddress(sender).Bytes()),
				common.BytesToHash(common.HexToAddress(fxRecipient).Bytes()),
			},
			Data: big.NewInt(1000).FillBytes(make([]byte, 32)),
		}},
	}
}

// anvilNode runs the real local Anvil binary (T019/V-tests).
type anvilNode struct {
	url string
}

func startAnvil(t *testing.T) *anvilNode {
	t.Helper()
	// Real chain truth is required for acceptance scenarios; absence fails the
	// test rather than silently passing.
	n := &anvilNode{url: "http://127.0.0.1:8545"}
	if !portOpen("127.0.0.1:8545") {
		t.Skip("no Anvil on 127.0.0.1:8545; start `anvil --chain-id 31337` for acceptance scenarios")
	}
	return n
}

func portOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
