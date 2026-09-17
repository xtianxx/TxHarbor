//go:build integration

package txlifecycle

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/signer"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

const jointChainID int64 = 31337

// jointEnv is the real joint stack: one 010-to-011 integration workspace with
// real PostgreSQL, a real Anvil node, the real in-process 009 signer-serve, the
// real 007/011 HTTP handlers and the real 010 Store.
type jointEnv struct {
	t         *testing.T
	ctx       context.Context
	dsn       string
	pool      *pgxpool.Pool
	anvilURL  string
	anvilRPC  *rpc.Client
	eth       *eth.Client
	store     *Store
	signer    *SignerClient
	devKey    *ecdsa.PrivateKey
	sender    string
	asset     string
	recipient string
	amount    string
	callerID  int64
	apiKey    string
	seq       int
}

func jointRPC(t *testing.T, out any, url, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := rpc.DialContext(ctx, url)
	if err != nil {
		t.Fatalf("dial anvil for %s: %v", method, err)
	}
	defer c.Close()
	if err := c.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("anvil %s: %v", method, err)
	}
}

func newJointEnv(t *testing.T) *jointEnv {
	t.Helper()
	ctx := context.Background()
	dsn := startPG(t)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

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
	anvilURL := fmt.Sprintf("http://%s:%s", host, port.Port())
	ethClient, err := eth.Dial(ctx, anvilURL, 10*time.Second)
	if err != nil {
		t.Fatalf("eth dial: %v", err)
	}
	t.Cleanup(ethClient.Close)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("dev key: %v", err)
	}
	sender := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	keyFile := filepath.Join(t.TempDir(), "joint.key")
	if err := os.WriteFile(keyFile, []byte(common.Bytes2Hex(crypto.FromECDSA(key))), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	var bal json.RawMessage
	jointRPC(t, &bal, anvilURL, "anvil_setBalance", common.HexToAddress(sender),
		hexutil.EncodeBig(new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)))

	j := &jointEnv{
		t: t, ctx: ctx, dsn: dsn, pool: pool, anvilURL: anvilURL,
		eth: ethClient, devKey: key, sender: sender,
		asset:     "0x2222222222222222222222222222222222222222",
		recipient: "0x3333333333333333333333333333333333333333",
		amount:    "1000",
		callerID:  1,
	}
	j.apiKey = jointIssueKey(t, ctx, pool, j.callerID)
	cred, err := signer.IssueCredential(ctx, pool, j.callerID, "joint")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	if err := signer.SetCanSign(ctx, pool, j.callerID, true); err != nil {
		t.Fatalf("SetCanSign: %v", err)
	}
	baseURL := startSignerServe(t, ctx, jointSignerEnv(t, dsn, keyFile, sender, j.asset, j.recipient))
	sc, err := NewSignerClient(SignerConfig{BaseURL: baseURL, Credential: cred, Timeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("NewSignerClient: %v", err)
	}
	j.signer = sc
	j.store = NewStore(pool).WithChain(ethClient, 15*time.Second).WithSigner(sc)

	j.mustExec(`INSERT INTO caller (caller_id, label, can_create) VALUES ($1,'joint',TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create = TRUE`, j.callerID)
	j.mustExec(`INSERT INTO indexer_lease (chain_id, owner_id, expires_at) VALUES ($1,'joint', now() + interval '1 hour')
		ON CONFLICT (chain_id) DO UPDATE SET expires_at = now() + interval '1 hour'`, jointChainID)
	j.mustExec(`INSERT INTO confirmation_policy_history (chain_id, policy_seq, threshold) VALUES ($1,1,1)
		ON CONFLICT (chain_id, policy_seq) DO NOTHING`, jointChainID)
	return j
}

func jointIssueKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) string {
	t.Helper()
	plaintext, _, err := withdrawal.IssueKey(ctx, pool, callerID, "joint")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	return plaintext
}

// jointSignerEnv is the real 009 environment for the joint asset/recipient.
func jointSignerEnv(t *testing.T, dsn, keyFile, sender, asset, recipient string) map[string]string {
	t.Helper()
	return map[string]string{
		config.EnvPGDSN:                 dsn,
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               strconv.FormatInt(jointChainID, 10),
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
		config.EnvReorgMaxDepth:         "100",
		config.EnvHTTPAddr:              "127.0.0.1:0",
		config.EnvSignerHTTPAddr:        "127.0.0.1:0",
		config.EnvSignerMode:            "development",
		config.EnvSignerKeyFile:         keyFile,
		config.EnvSignerChains:          strconv.FormatInt(jointChainID, 10),
		config.EnvSignerSenders:         sender,
		config.EnvSignerAssets:          asset,
		config.EnvSignerRecipients:      recipient,
		config.EnvSignerMaxAmount:       "2000000",
		config.EnvSignerMaxGasLimit:     "100000",
		config.EnvSignerMaxFeePerGas:    "2000000000",
		config.EnvSignerMaxPriorityFee:  "1500000000",
		config.EnvSignerMaxGasPrice:     "2000000000",
		config.EnvNonceReadToken:        "joint-nonce-read-token",
	}
}

func (j *jointEnv) mustExec(query string, args ...any) {
	j.t.Helper()
	if _, err := j.pool.Exec(j.ctx, query, args...); err != nil {
		j.t.Fatalf("exec %q: %v", query, err)
	}
}

func (j *jointEnv) anvilHead() uint64 {
	j.t.Helper()
	var n hexutil.Uint64
	jointRPC(j.t, &n, j.anvilURL, "eth_blockNumber")
	return uint64(n)
}

func (j *jointEnv) anvilBlockHash(number uint64) string {
	j.t.Helper()
	var blk struct {
		Hash string `json:"hash"`
	}
	jointRPC(j.t, &blk, j.anvilURL, "eth_getBlockByNumber", hexutil.EncodeUint64(number), false)
	return blk.Hash
}

// waitMined polls the real node until the dispatch hash has a receipt.
func (j *jointEnv) waitMined(hash string) {
	j.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if r, err := j.eth.TransactionReceipt(j.ctx, common.HexToHash(hash)); err == nil && r != nil {
			return
		}
		if time.Now().After(deadline) {
			j.t.Fatalf("tx %s was not mined", hash)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// seedChainTruth mirrors Anvil blocks into chain_blocks so canonicality and
// confirmation have real chain truth to compare against (test-owned 002 rows).
func (j *jointEnv) seedChainTruth() {
	j.t.Helper()
	head := j.anvilHead()
	for n := uint64(0); n <= head; n++ {
		j.mustExec(`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
			VALUES ($1,$2,$3,$3,TRUE) ON CONFLICT DO NOTHING`,
			jointChainID, n, strings.ToLower(j.anvilBlockHash(n)))
	}
}

// installTransferEmit puts fallback bytecode at the asset address that emits
// exactly the expected ERC-20 Transfer log for the joint sender/recipient.
func (j *jointEnv) installTransferEmit(amount int64) {
	j.t.Helper()
	code := jointEmitCode(j.sender, j.recipient, big.NewInt(amount))
	var ok bool
	jointRPC(j.t, &ok, j.anvilURL, "anvil_setCode", common.HexToAddress(j.asset), hexutil.Encode(code))
}

func jointEmitCode(sender, recipient string, amount *big.Int) []byte {
	topics := [3]common.Hash{
		crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)")),
		common.BytesToHash(common.HexToAddress(sender).Bytes()),
		common.BytesToHash(common.HexToAddress(recipient).Bytes()),
	}
	var code []byte
	code = append(code, 0x7f)
	code = append(code, common.BigToHash(amount).Bytes()...)
	code = append(code, 0x60, 0x00, 0x52)
	for _, topic := range [3]common.Hash{topics[2], topics[1], topics[0]} {
		code = append(code, 0x7f)
		code = append(code, topic.Bytes()...)
	}
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0xa3, 0x00)
	return code
}

// jointIntent is one admitted intent with its real claim and 007 request.
type jointIntent struct {
	requestID        string
	intentID         string
	authorizationID  string
	signingRequestID string
	bindingID        string
	attemptID        string
	claimVersion     int64
	ownerID          string
}

// admit creates the 007 request over real HTTP, the 011 intent over real HTTP,
// and the real 011 execution claim; then it seeds the 008 binding and persists
// the 010 attempt (T1). It returns the joint identity.
func (j *jointEnv) admit() *jointIntent {
	j.t.Helper()
	j.seq++
	n := j.seq
	authID := fmt.Sprintf("wa-j%d", n)
	intentID := fmt.Sprintf("intent-j%d", n)
	bindingID := fmt.Sprintf("bind-j%d", n)
	attemptID := fmt.Sprintf("att-j%d", n)
	ownerID := fmt.Sprintf("worker-j%d", n)

	j.mustExec(`INSERT INTO deposit_config_history (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, request_id)
		VALUES ($1,$2,$3,0,$4,$5,0,$6) ON CONFLICT DO NOTHING`,
		jointChainID, n, strings.Repeat("a", 64), j.asset+":0", j.recipient+":0", fmt.Sprintf("cfg-%d", n))

	if _, err := withdrawal.SupplyGrant(j.ctx, j.pool, withdrawal.OpInput{
		OperationID: fmt.Sprintf("op-j%d", n), Action: "supply",
		AuthorizationID: authID, CallerID: j.callerID, ChainID: jointChainID,
		Asset: j.asset, Recipient: j.recipient, Amount: j.amount,
	}, "joint", "seed"); err != nil {
		j.t.Fatalf("SupplyGrant: %v", err)
	}
	if _, err := j.pool.Exec(j.ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'joint') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, j.callerID); err != nil {
		j.t.Fatalf("seed permission: %v", err)
	}
	j.mustExec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq) VALUES ($1,$2,'active',1)
		ON CONFLICT (chain_id, sender) DO UPDATE SET state='active'`, jointChainID, j.sender)
	j.mustExec(`INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1,$2)
		ON CONFLICT (chain_id, sender) DO NOTHING`, jointChainID, j.sender)

	createSrv := httptest.NewServer(&app.WithdrawalHandler{Pool: j.pool, ChainID: jointChainID})
	defer createSrv.Close()
	body, _ := json.Marshal(map[string]any{
		"idempotency_key": fmt.Sprintf("idem-j%d", n), "chain_id": jointChainID,
		"asset": j.asset, "recipient": j.recipient, "amount": j.amount, "authorization_id": authID,
	})
	status, raw := jointHTTPDo(j.t, http.MethodPost, createSrv.URL+"/withdrawals", j.apiKey, string(body))
	if status != http.StatusOK && status != http.StatusCreated {
		j.t.Fatalf("007 create status=%d body=%s", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.RequestID == "" {
		j.t.Fatalf("007 create body=%s err=%v", raw, err)
	}
	signingRequestID := created.RequestID
	j.mustExec(`INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,FALSE,1,'joint')`,
		authID, intentID, signingRequestID, j.sender, int64(1e15), int64(2e9), int64(15e8))

	admitSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux := http.NewServeMux()
		mux.Handle("POST /withdrawals/{request_id}/execution",
			&app.WithdrawalExecutionHandler{Pool: j.pool, ChainID: jointChainID})
		mux.ServeHTTP(w, r)
	}))
	defer admitSrv.Close()
	status, raw = jointHTTPDo(j.t, http.MethodPost, admitSrv.URL+"/withdrawals/"+created.RequestID+"/execution", j.apiKey, "")
	if status != http.StatusCreated {
		j.t.Fatalf("011 admit status=%d body=%s", status, raw)
	}
	var admitted struct {
		IntentID string `json:"intent_id"`
	}
	if err := json.Unmarshal(raw, &admitted); err != nil || admitted.IntentID == "" {
		j.t.Fatalf("011 admit body=%s err=%v", raw, err)
	}
	if admitted.IntentID != intentID {
		j.t.Fatalf("admitted intent=%s want %s", admitted.IntentID, intentID)
	}

	claims, err := execution.NewClaimStore(j.pool, 30*time.Second, 5*time.Minute)
	if err != nil {
		j.t.Fatalf("NewClaimStore: %v", err)
	}
	claim, err := claims.Claim(j.ctx, intentID, ownerID)
	if err != nil || !claim.Acquired {
		j.t.Fatalf("claim: %+v %v", claim, err)
	}

	authAnchor := strings.TrimPrefix(crypto.Keccak256Hash([]byte(authID)).Hex(), "0x")
	j.mustExec(`INSERT INTO nonce_bindings (binding_id, intent_id, chain_id, sender, nonce, state, authorization_id, authorization_version, registry_seq, allocation_observation_id)
		VALUES ($1,$2,$3,$4,0,'allocated',$5,$6,1,'obs-joint')`, bindingID, intentID, jointChainID, j.sender, authID, authAnchor)

	req := &PrepareRequest{
		AttemptID: attemptID, SigningRequestID: signingRequestID, IntentID: intentID,
		BindingRef: bindingID, AuthorizationID: authID, AuthorizationVersion: 1,
		RecoveryVersion: 0, ChainID: uint64(jointChainID), Sender: j.sender, Nonce: "0",
		TxType: TxTypeDynamicFee, GasLimit: "100000",
		MaxFeePerGas: "1000000000", MaxPriorityFeePerGas: "100000000",
		Asset: j.asset, Recipient: j.recipient, Amount: j.amount,
	}
	if _, err := j.store.PrepareAttempt(j.ctx, req); err != nil {
		j.t.Fatalf("PrepareAttempt: %v", err)
	}
	return &jointIntent{
		requestID: created.RequestID, intentID: intentID, authorizationID: authID,
		signingRequestID: signingRequestID, bindingID: bindingID, attemptID: attemptID,
		claimVersion: claim.Version, ownerID: ownerID,
	}
}

func (j *jointEnv) claim(jj *jointIntent) ClaimRef {
	return ClaimRef{IntentID: jj.intentID, WorkerID: jj.ownerID, LeaseVersion: jj.claimVersion}
}

func jointHTTPDo(t *testing.T, method, url, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, raw
}
