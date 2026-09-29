//go:build e2e

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/eth"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/nonce"
	"github.com/xtianxx/txharbor/internal/recovery"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore"
	"github.com/xtianxx/txharbor/internal/recovery/controlstore/schema"
	"github.com/xtianxx/txharbor/internal/signer"
	"github.com/xtianxx/txharbor/internal/txlifecycle"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// TestE2ERecoveryRestoredWorkerDoesNotRepeatBroadcast runs the actual 011
// WithdrawalWorker, the production 010 lifecycle adapter, the production
// 008 nonce allocator and the real 009 SignerServe against PostgreSQL and
// Anvil. Its archive is deliberately taken AFTER a successful broadcast: this
// proves a restored worker retry from durable signed/sent state does not create
// another payment. It does not claim safety for restoring a point from before
// signing/broadcast, nor does it synthesize an unknown signer outcome.
func TestE2ERecoveryRestoredWorkerDoesNotRepeatBroadcast(t *testing.T) {
	ctx := context.Background()
	dsn, postgresID := e2eRecoveryPostgres(t)
	anvilURL := e2eStartAnvil(t)
	pool := e2eOpenPool(t, dsn)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate disposable signer key: %v", err)
	}
	sender := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	const asset = "0x2222222222222222222222222222222222222222"
	const recipient = "0x3333333333333333333333333333333333333333"
	const amount = "1000"
	const callerID int64 = 701
	keyFile := filepath.Join(t.TempDir(), "signer.key")
	if err := os.WriteFile(keyFile, []byte(common.Bytes2Hex(crypto.FromECDSA(key))), 0o600); err != nil {
		t.Fatalf("write disposable signer key: %v", err)
	}
	anvilRPC, err := rpc.DialContext(ctx, anvilURL)
	if err != nil {
		t.Fatalf("dial Anvil: %v", err)
	}
	defer anvilRPC.Close()
	setAnvilBalance(t, anvilRPC, common.HexToAddress(sender), new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil))
	installRecoveryTransferEmitter(t, anvilRPC, sender, recipient, big.NewInt(1000))

	// The signer identity/capability and withdrawal authorization are real
	// service writes. SQL below seeds only deployment policy and nonce registry
	// configuration, never intent/attempt/signing/broadcast state.
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id,label,can_create) VALUES ($1,'recovery-worker-e2e',TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create=TRUE`, callerID); err != nil {
		t.Fatalf("seed caller config: %v", err)
	}
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, callerID, "recovery-worker-e2e")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id,can_execute,updated_by)
		VALUES ($1,TRUE,'recovery-worker-e2e') ON CONFLICT (caller_id) DO UPDATE SET can_execute=TRUE`, callerID); err != nil {
		t.Fatalf("seed execution permission: %v", err)
	}
	if _, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID: "recovery-worker-supply", Action: "supply", AuthorizationID: "recovery-worker-auth",
		CallerID: callerID, ChainID: 31337, Asset: asset, Recipient: recipient, Amount: amount,
	}, "recovery-worker-e2e", "controlled local e2e authorization"); err != nil {
		t.Fatalf("SupplyGrant: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO deposit_config_history (chain_id,version_seq,config_hash,start_block,assets,watches,replay_from,request_id)
		VALUES (31337,1,$1,0,$2,$3,0,'recovery-worker-e2e')`, strings.Repeat("b", 64), asset+":0", recipient+":0"); err != nil {
		t.Fatalf("seed asset policy config: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_wallet_registry (chain_id,sender,state,registry_seq)
		VALUES (31337,$1,'active',1) ON CONFLICT (chain_id,sender) DO UPDATE SET state='active'`, sender); err != nil {
		t.Fatalf("seed nonce registry config: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_scope_state (chain_id,sender) VALUES (31337,$1)
		ON CONFLICT (chain_id,sender) DO NOTHING`, sender); err != nil {
		t.Fatalf("seed nonce scope: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO confirmation_policy_history (chain_id,policy_seq,threshold)
		VALUES (31337,1,1) ON CONFLICT (chain_id,policy_seq) DO NOTHING`); err != nil {
		t.Fatalf("seed confirmation policy config: %v", err)
	}

	credential, err := signer.IssueCredential(ctx, pool, callerID, "recovery-worker-e2e")
	if err != nil {
		t.Fatalf("IssueCredential: %v", err)
	}
	if err := signer.SetCanSign(ctx, pool, callerID, true); err != nil {
		t.Fatalf("SetCanSign: %v", err)
	}
	signerEnv := e2eBaseEnv(dsn, anvilURL, e2eFreeAddr(t), []string{"127.0.0.1:9092"})
	signerEnv[config.EnvSignerHTTPAddr] = e2eFreeAddr(t)
	signerEnv[config.EnvSignerMode] = "development"
	signerEnv[config.EnvSignerKeyFile] = keyFile
	signerEnv[config.EnvSignerChains] = "31337"
	signerEnv[config.EnvSignerSenders] = sender
	signerEnv[config.EnvSignerAssets] = asset
	signerEnv[config.EnvSignerRecipients] = recipient
	signerEnv[config.EnvSignerMaxAmount] = "2000000"
	signerEnv[config.EnvSignerMaxGasLimit] = "100000"
	signerEnv[config.EnvSignerMaxFeePerGas] = "2000000000"
	signerEnv[config.EnvSignerMaxPriorityFee] = "1500000000"
	signerEnv[config.EnvSignerMaxGasPrice] = "2000000000"
	signerEnv[config.EnvNonceReadToken] = "recovery-worker-nonce-read"
	signerURL, stopSigner := e2eRecoverySigner(t, signerEnv)
	defer stopSigner()

	// Use the supported HTTP handlers for accepted request and intent
	// admission. No payment-intent or lifecycle rows are inserted directly.
	createMux := http.NewServeMux()
	createMux.Handle("POST /withdrawals", &WithdrawalHandler{Pool: pool, ChainID: 31337})
	create := httptest.NewServer(createMux)
	body, _ := json.Marshal(map[string]any{
		"idempotency_key": "recovery-worker-idem", "chain_id": 31337, "asset": asset,
		"recipient": recipient, "amount": amount, "authorization_id": "recovery-worker-auth",
	})
	status, raw := e2eJSONDo(t, http.MethodPost, create.URL+"/withdrawals", apiKey, string(body))
	create.Close()
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("007 create status=%d body=%s", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.RequestID == "" {
		t.Fatalf("create response=%s err=%v", raw, err)
	}
	var intentID string
	if err := pool.QueryRow(ctx, `SELECT intent_id FROM payment_intents WHERE request_id=$1`, created.RequestID).Scan(&intentID); err == nil {
		t.Fatalf("intent %s existed before execution admission; unexpected transition", intentID)
	} else if err != pgx.ErrNoRows {
		t.Fatalf("check pre-admission intent absence: %v", err)
	}
	// Scope is authorization configuration binding, not a state transition.
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id,intent_id,request_id,sender,fee_max_total,fee_max_per_gas,fee_max_priority,allows_fee_replacement,authorization_version,attested_by)
		VALUES ('recovery-worker-auth','intent-recovery-worker',$1,$2,$3,$4,$5,TRUE,1,'e2e')`,
		created.RequestID, sender, int64(1e15), int64(2e9), int64(15e8)); err != nil {
		t.Fatalf("seed authorization scope config: %v", err)
	}
	admitMux := http.NewServeMux()
	admitMux.Handle("POST /withdrawals/{request_id}/execution", &WithdrawalExecutionHandler{Pool: pool, ChainID: 31337})
	admit := httptest.NewServer(admitMux)
	status, raw = e2eJSONDo(t, http.MethodPost, admit.URL+"/withdrawals/"+created.RequestID+"/execution", apiKey, "")
	admit.Close()
	if status != http.StatusCreated {
		t.Fatalf("011 admission status=%d body=%s", status, raw)
	}
	var admitted struct {
		IntentID string `json:"intent_id"`
	}
	if err := json.Unmarshal(raw, &admitted); err != nil || admitted.IntentID == "" {
		t.Fatalf("admission response=%s err=%v", raw, err)
	}
	if admitted.IntentID != "intent-recovery-worker" {
		t.Fatalf("admitted intent=%q, want configured authorization intent", admitted.IntentID)
	}
	var accepted string
	if err := pool.QueryRow(ctx, `SELECT status FROM withdrawal_requests WHERE request_id=$1`, created.RequestID).Scan(&accepted); err != nil || accepted != "accepted" {
		t.Fatalf("request status=%q err=%v, want accepted", accepted, err)
	}

	workerCfg := &config.Config{
		ChainID: 31337, RPCURL: anvilURL, TxSignerURL: signerURL, TxSignerCredential: credential,
		TxSendTimeout: 10 * time.Second, ProbeTimeout: 5 * time.Second, NonceReadToken: "recovery-worker-nonce-read",
		WorkerTTL: 30 * time.Second, WorkerHeartbeat: time.Second, WorkerStall: 5 * time.Minute,
		WorkerBackoffBase: 50 * time.Millisecond, WorkerBackoffMax: time.Second,
		WorkerScanInterval: 50 * time.Millisecond, WorkerLabel: "recovery-worker-e2e",
	}
	// Preserve a genuine pre-effect recovery point containing the admitted
	// intent but no claim, signing request, signature or dispatch.
	preEffectArchive := filepath.Join(t.TempDir(), "worker-before-effect.dump")
	e2eRecoveryDump(t, postgresID, dsn, preEffectArchive)
	// Forward to real Anvil, accept exactly one real eth_sendRawTransaction,
	// then drop only its JSON-RPC response. This exercises 010's unknown
	// dispatch classification without substituting a stub for chain execution.
	dispatchProxy := e2eRecoveryDropDispatchProxy(t, anvilURL)
	workerCfg.RPCURL = dispatchProxy.URL
	workerCfg.WorkerScanInterval = 2 * time.Second // allow inspection before the next reconcile cycle
	chain := dialRecoveryEth(t, anvilURL)
	defer chain.Close()
	worker := e2eRecoveryWorker(t, ctx, pool, workerCfg)
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); worker.Run(workerCtx) }()

	var attemptID, txHash string
	e2eWait(t, 45*time.Second, "production worker's real dispatch recorded unknown after response loss", func() bool {
		err := pool.QueryRow(ctx, `SELECT a.attempt_id, COALESCE(s.tx_hash,'')
			FROM tx_attempts a LEFT JOIN tx_attempt_signings s ON s.attempt_id=a.attempt_id
			WHERE a.intent_id=$1 AND a.state='unknown'
			ORDER BY a.created_at DESC LIMIT 1`, admitted.IntentID).Scan(&attemptID, &txHash)
		if err != nil || txHash == "" {
			return false
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempt_signings WHERE attempt_id=$1`, attemptID).Scan(&n); err != nil || n != 1 {
			return false
		}
		got, receiptErr := chain.TransactionReceipt(ctx, common.HexToHash(txHash))
		return receiptErr == nil && got != nil && got.Status == types.ReceiptStatusSuccessful
	})
	// The real Anvil node has accepted and mined the transaction, but the worker
	// observed only a lost response and durably classified the attempt unknown.
	// Stop before its next probe so this snapshot preserves the uncertainty.
	stopWorker()
	<-workerDone
	if txHash == "" {
		t.Fatal("production worker did not persist the unknown dispatch's signed hash")
	}
	var dispatchState string
	if err := pool.QueryRow(ctx, `SELECT state FROM tx_attempts WHERE attempt_id=$1`, attemptID).Scan(&dispatchState); err != nil || dispatchState != "unknown" {
		t.Fatalf("dispatch state=%q err=%v, want unknown after real acceptance/response loss", dispatchState, err)
	}
	var signedBytes []byte
	var signature string
	if err := pool.QueryRow(ctx, `SELECT signed_tx_bytes,signature FROM tx_attempt_signings WHERE attempt_id=$1`, attemptID).Scan(&signedBytes, &signature); err != nil {
		t.Fatalf("read production signer output: %v", err)
	}
	if len(signedBytes) == 0 || signature == "" || !strings.EqualFold(crypto.Keccak256Hash(signedBytes).Hex(), txHash) {
		t.Fatalf("persisted signed stage is incomplete or identity differs: bytes=%d signature=%t hash=%s tx_hash=%s", len(signedBytes), signature != "", crypto.Keccak256Hash(signedBytes), txHash)
	}
	hash := common.HexToHash(txHash)
	receipt, err := chain.TransactionReceipt(ctx, hash)
	if err != nil || receipt.Status != types.ReceiptStatusSuccessful {
		t.Fatalf("real broadcast receipt=%+v err=%v", receipt, err)
	}
	var nonce hexutil.Uint64
	recoveryRPC(t, anvilRPC, &nonce, "eth_getTransactionCount", common.HexToAddress(sender), "latest")
	var senderBalance hexutil.Big
	recoveryRPC(t, anvilRPC, &senderBalance, "eth_getBalance", common.HexToAddress(sender), "latest")
	confirmedTx, _, err := chain.TransactionByHash(ctx, hash)
	if err != nil {
		t.Fatalf("read confirmed transaction: %v", err)
	}
	if uint64(nonce) != confirmedTx.Nonce()+1 {
		t.Fatalf("chain nonce=%d after transaction nonce=%d, want one consumed nonce", nonce, confirmedTx.Nonce())
	}
	if receipt.TxHash != hash || receipt.GasUsed == 0 {
		t.Fatalf("receipt identity/gas = %s/%d, want %s/nonzero", receipt.TxHash, receipt.GasUsed, hash)
	}
	transferTopic := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	if len(receipt.Logs) != 1 || receipt.Logs[0].Address != common.HexToAddress(asset) ||
		len(receipt.Logs[0].Topics) != 3 || receipt.Logs[0].Topics[0] != transferTopic ||
		receipt.Logs[0].Topics[1] != common.BytesToHash(common.HexToAddress(sender).Bytes()) ||
		receipt.Logs[0].Topics[2] != common.BytesToHash(common.HexToAddress(recipient).Bytes()) ||
		new(big.Int).SetBytes(receipt.Logs[0].Data).Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("real transfer receipt log does not match sender/recipient/amount: %+v", receipt.Logs)
	}
	var broadcastAttempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id=$1`, admitted.IntentID).Scan(&broadcastAttempts); err != nil || broadcastAttempts != 1 {
		t.Fatalf("real worker attempts=%d err=%v, want exactly one", broadcastAttempts, err)
	}

	// First restore the pre-effect point. The real recovery gate is configured
	// for an open recovery instance with no verified release, so the worker must
	// refuse the stale admitted intent instead of choosing a new nonce/payment.
	preRestoredDSN := e2eRecoveryRestore(t, postgresID, dsn, preEffectArchive, "worker_pre_effect_recovered")
	preRestoredPool := e2eOpenPool(t, preRestoredDSN)
	controlDSN, instanceID := e2eRecoveryControlStore(t, dsn)
	gateCfg := *workerCfg
	gateCfg.PGDSN = dsn
	gateCfg.Recovery.ControlDSN = controlDSN
	gateCfg.Recovery.Principal = "deploy:drill-executor"
	gateCfg.Recovery.GateTTL = time.Minute
	gateEnv := map[string]string{
		config.EnvRecoveryControlDSN: controlDSN,
		config.EnvRecoveryInstance:   instanceID,
		config.EnvRecoveryPrincipal:  "deploy:drill-executor",
		config.EnvRecoveryGateTTL:    time.Minute.String(),
	}
	recoveryWiring, err := assembleExistingWithdrawalRecovery(ctx, &gateCfg, e2eGetenv(gateEnv))
	if err != nil {
		t.Fatalf("assemble active recovery gate: %v", err)
	}
	t.Cleanup(recoveryWiring.close)
	blocked, err := recoveryWiring.admit(ctx, recovery.CapabilityExistingWithdrawalRecovery, "worker_claim", admitted.IntentID)
	if err != nil || blocked.Allowed || !blocked.RefusalClass.Known() || blocked.RefusalClass == recovery.RefusalNoInstance {
		t.Fatalf("pre-effect restored retry admission=%+v err=%v; want a classified fail-closed recovery refusal", blocked, err)
	}
	preWorker := e2eRecoveryWorker(t, ctx, preRestoredPool, &gateCfg)
	preWorker.Recovery = recoveryWiring
	preCtx, stopPre := context.WithCancel(ctx)
	preDone := make(chan struct{})
	go func() { defer close(preDone); preWorker.Run(preCtx) }()
	time.Sleep(500 * time.Millisecond)
	stopPre()
	<-preDone
	var preAttempts, preClaims int
	if err := preRestoredPool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id=$1`, admitted.IntentID).Scan(&preAttempts); err != nil {
		t.Fatalf("pre-effect restored attempt count: %v", err)
	}
	if err := preRestoredPool.QueryRow(ctx, `SELECT count(*) FROM execution_claims WHERE intent_id=$1`, admitted.IntentID).Scan(&preClaims); err != nil {
		t.Fatalf("pre-effect restored claim count: %v", err)
	}
	if preAttempts != 0 || preClaims != 0 {
		t.Fatalf("blocked pre-effect retry wrote attempts=%d claims=%d, want zero", preAttempts, preClaims)
	}
	var blockedNonce hexutil.Uint64
	recoveryRPC(t, anvilRPC, &blockedNonce, "eth_getTransactionCount", common.HexToAddress(sender), "latest")
	var blockedBalance hexutil.Big
	recoveryRPC(t, anvilRPC, &blockedBalance, "eth_getBalance", common.HexToAddress(sender), "latest")
	if blockedNonce != nonce || (*big.Int)(&blockedBalance).Cmp((*big.Int)(&senderBalance)) != 0 {
		t.Fatalf("blocked pre-effect retry changed chain nonce/balance: %d/%s -> %d/%s", nonce, (*big.Int)(&senderBalance), blockedNonce, (*big.Int)(&blockedBalance))
	}
	blockedReceipt, err := chain.TransactionReceipt(ctx, hash)
	if err != nil || blockedReceipt.TxHash != receipt.TxHash || blockedReceipt.BlockNumber.Cmp(receipt.BlockNumber) != 0 {
		t.Fatalf("pre-effect blocked retry altered/lost original receipt: before=%+v after=%+v err=%v", receipt, blockedReceipt, err)
	}

	// Also restore the uncertain post-dispatch snapshot. A fresh worker must
	// keep that durable uncertainty distinct and must not dispatch again. This
	// case asserts preservation of unknown, not a claim that unknown was closed.
	archive := filepath.Join(t.TempDir(), "worker-after-unknown.dump")
	e2eRecoveryDump(t, postgresID, dsn, archive)
	restoredDSN := e2eRecoveryRestore(t, postgresID, dsn, archive, "worker_unknown_recovered")
	restoredPool := e2eOpenPool(t, restoredDSN)
	restoredWorker := e2eRecoveryWorker(t, ctx, restoredPool, workerCfg)
	retryCtx, stopRetry := context.WithCancel(ctx)
	retryDone := make(chan struct{})
	go func() { defer close(retryDone); restoredWorker.Run(retryCtx) }()
	time.Sleep(2 * time.Second) // several real scan/claim cycles after restore
	stopRetry()
	<-retryDone

	var restoredAttemptCount int
	if err := restoredPool.QueryRow(ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id=$1`, admitted.IntentID).Scan(&restoredAttemptCount); err != nil {
		t.Fatalf("restored attempt count: %v", err)
	}
	if restoredAttemptCount != 1 {
		t.Fatalf("restored retry produced %d attempts, want the one persisted broadcast", restoredAttemptCount)
	}
	var restoredHash string
	var restoredSignedBytes []byte
	if err := restoredPool.QueryRow(ctx, `SELECT tx_hash,signed_tx_bytes FROM tx_attempt_signings WHERE attempt_id=$1`, attemptID).Scan(&restoredHash, &restoredSignedBytes); err != nil {
		t.Fatalf("restored signed identity: %v", err)
	}
	if !strings.EqualFold(restoredHash, txHash) || !bytes.Equal(restoredSignedBytes, signedBytes) {
		t.Fatalf("restored transaction identity/bytes differ: hash=%s original=%s bytes_equal=%t", restoredHash, txHash, bytes.Equal(restoredSignedBytes, signedBytes))
	}
	var retryNonce hexutil.Uint64
	recoveryRPC(t, anvilRPC, &retryNonce, "eth_getTransactionCount", common.HexToAddress(sender), "latest")
	var retryBalance hexutil.Big
	recoveryRPC(t, anvilRPC, &retryBalance, "eth_getBalance", common.HexToAddress(sender), "latest")
	if retryNonce != nonce {
		t.Fatalf("sender nonce changed after restored worker cycles: %d -> %d", nonce, retryNonce)
	}
	if (*big.Int)(&retryBalance).Cmp((*big.Int)(&senderBalance)) != 0 {
		t.Fatalf("sender balance changed after restored worker cycles: %s -> %s", (*big.Int)(&senderBalance), (*big.Int)(&retryBalance))
	}
	retryReceipt, err := chain.TransactionReceipt(ctx, hash)
	if err != nil || retryReceipt.TxHash != receipt.TxHash || retryReceipt.BlockNumber.Cmp(receipt.BlockNumber) != 0 {
		t.Fatalf("receipt changed across restored worker retry: before=%+v after=%+v err=%v", receipt, retryReceipt, err)
	}
	var finalState string
	if err := restoredPool.QueryRow(ctx, `SELECT state FROM tx_attempts WHERE attempt_id=$1`, attemptID).Scan(&finalState); err != nil {
		t.Fatalf("post-restore final attempt state: %v", err)
	}
	if finalState != "unknown" {
		t.Fatalf("restored worker state=%q, want the durable unknown classification preserved", finalState)
	}
	t.Logf("evidence classes: accepted intent=%s; signed+persisted; real Anvil broadcast=%s receipt=%s nonce=%d; response-loss state=unknown; pre-effect restore blocked=%s with zero claim/attempt; post-unknown restored worker preserved state=%s without another payment", admitted.IntentID, txHash, receipt.BlockNumber, nonce, blocked.RefusalClass, finalState)
}

func e2eRecoveryPostgres(t *testing.T) (dsn, containerID string) {
	t.Helper()
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18.6-trixie",
		postgres.WithDatabase("txharbor"), postgres.WithUsername("txharbor"), postgres.WithPassword("txharbor"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)))
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("PostgreSQL DSN: %v", err)
	}
	if err := db.MigrateUp(ctx, db.MigrateOptions{DSN: dsn, LockTimeout: 30 * time.Second, ConnectTimeout: 10 * time.Second}, io.Discard); err != nil {
		t.Fatalf("migrate PostgreSQL: %v", err)
	}
	return dsn, ctr.GetContainerID()
}

func e2eRecoverySigner(t *testing.T, env map[string]string) (string, func()) {
	t.Helper()
	addr := env[config.EnvSignerHTTPAddr]
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	var stderr bytes.Buffer
	go func() {
		done <- SignerServe(ctx, nil, Deps{Getenv: e2eGetenv(env), Stdout: io.Discard, Stderr: &stderr})
	}()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			_ = conn.Close()
			return "http://" + addr, func() {
				cancel()
				select {
				case code := <-done:
					if code != 0 {
						t.Errorf("SignerServe exit=%d stderr=%s", code, stderr.String())
					}
				case <-time.After(15 * time.Second):
					t.Error("SignerServe failed to stop")
				}
			}
		}
		select {
		case code := <-done:
			t.Fatalf("SignerServe exited early code=%d stderr=%s", code, stderr.String())
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	t.Fatalf("SignerServe listener did not open: %s", stderr.String())
	return "", func() {}
}

func e2eRecoveryWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) *WithdrawalWorker {
	t.Helper()
	chain, err := eth.Dial(ctx, cfg.RPCURL, cfg.ProbeTimeout)
	if err != nil {
		t.Fatalf("dial worker chain: %v", err)
	}
	t.Cleanup(chain.Close)
	signerClient, err := txlifecycle.NewSignerClient(txlifecycle.SignerConfig{
		BaseURL: cfg.TxSignerURL, Credential: cfg.TxSignerCredential, Timeout: cfg.TxSendTimeout,
	})
	if err != nil {
		t.Fatalf("production signer client: %v", err)
	}
	store := txlifecycle.NewStore(pool).WithChain(chain, cfg.TxSendTimeout).WithSigner(signerClient)
	adapter, err := txlifecycle.NewLifecycleLive(pool, store)
	if err != nil {
		t.Fatalf("production lifecycle adapter: %v", err)
	}
	anvilRPC, err := rpc.DialContext(ctx, cfg.RPCURL)
	if err != nil {
		t.Fatalf("dial allocator chain: %v", err)
	}
	t.Cleanup(anvilRPC.Close)
	gate := nonce.NewRebuildGate()
	gate.Open()
	allocator := nonce.NewAllocator(pool, nonce.NewObserver(anvilRPC, nonce.ObserverConfig{
		RPCTimeout: cfg.ProbeTimeout, RetryInitial: 10 * time.Millisecond, RetryMax: 100 * time.Millisecond,
	}), gate)
	deps := JointDeps{
		Advancer: adapter, Reader: adapter,
		Binding: &e2eRecoveryBinding{provider: nonce.NewReadProvider(pool, cfg.NonceReadToken)},
		AllocBinding: func(ctx context.Context, intentID string) (string, error) {
			var sender, authID string
			if err := pool.QueryRow(ctx, `SELECT sender,authorization_id FROM payment_intents WHERE intent_id=$1`, intentID).Scan(&sender, &authID); err != nil {
				return "", err
			}
			_, outcome, err := allocator.Allocate(ctx, nonce.AllocationRequest{IntentID: intentID, ChainID: int64(cfg.ChainID), Sender: sender, AuthorizationID: authID})
			return string(outcome), err
		},
		ConfirmAttempt: func(ctx context.Context, intentID string) error {
			var attemptID, state string
			err := pool.QueryRow(ctx, `SELECT attempt_id,state FROM tx_attempts WHERE intent_id=$1 ORDER BY created_at DESC,attempt_id DESC LIMIT 1`, intentID).Scan(&attemptID, &state)
			if err != nil {
				if err == pgx.ErrNoRows {
					return nil
				}
				return err
			}
			if state == "prepared" || state == "signed" {
				return nil
			}
			_, err = store.Reconcile(ctx, attemptID, "")
			return err
		},
	}
	worker, err := NewJointWithdrawalWorker(pool, cfg, nil, nil, deps)
	if err != nil {
		t.Fatalf("construct production joint worker: %v", err)
	}
	return worker
}

func e2eRecoveryDump(t *testing.T, containerID, sourceDSN, path string) {
	t.Helper()
	internal := e2eRecoveryInternalDSN(t, sourceDSN)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create backup archive: %v", err)
	}
	defer f.Close()
	cmd := exec.Command("docker", "exec", containerID, "pg_dump", "--format=custom", "--dbname="+internal)
	cmd.Stdout, cmd.Stderr = f, io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("real pg_dump: %v", err)
	}
}

func e2eRecoveryRestore(t *testing.T, containerID, sourceDSN, archive, targetName string) string {
	t.Helper()
	parsed, err := url.Parse(sourceDSN)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	parsed.Path = "/" + targetName
	adminDSN := parsed.String()
	internalTarget := e2eRecoveryInternalDSN(t, adminDSN)
	internalSource := e2eRecoveryInternalDSN(t, sourceDSN)
	cmdCreate := exec.Command("docker", "exec", containerID, "createdb", "--maintenance-db="+internalSource, targetName)
	if output, err := cmdCreate.CombinedOutput(); err != nil {
		t.Fatalf("create restore target: %v: %s", err, output)
	}
	f, err := os.Open(archive)
	if err != nil {
		t.Fatalf("open backup archive: %v", err)
	}
	defer f.Close()
	cmd := exec.Command("docker", "exec", "-i", containerID, "pg_restore", "--no-owner", "--dbname="+internalTarget)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = f, io.Discard, io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("real pg_restore: %v", err)
	}
	return adminDSN
}

func e2eRecoveryControlStore(t *testing.T, dataDSN string) (string, string) {
	t.Helper()
	ctx := context.Background()
	dataTarget, err := controlstore.ParseDSNTarget(dataDSN)
	if err != nil {
		t.Fatalf("parse authoritative e2e data DSN for recovery target binding: %v", err)
	}
	targetGuardKey, err := controlstore.TargetGuardKey(dataTarget)
	if err != nil {
		t.Fatalf("derive authoritative e2e target guard key: %v", err)
	}
	dataURL, err := url.Parse(dataDSN)
	if err != nil {
		t.Fatalf("parse data DSN for control database: %v", err)
	}
	dataPool := e2eOpenPool(t, dataDSN)
	if _, err := dataPool.Exec(ctx, `CREATE DATABASE recovery_worker_control`); err != nil {
		t.Fatalf("create separate recovery control database: %v", err)
	}
	controlURL := *dataURL
	controlURL.Path = "/recovery_worker_control"
	controlDSN := controlURL.String()
	if err := db.MigrateUp(ctx, db.MigrateOptions{
		DSN: controlDSN, LockTimeout: 20 * time.Second, ConnectTimeout: 10 * time.Second, FS: schema.FS,
	}, io.Discard); err != nil {
		t.Fatalf("migrate independent recovery control store: %v", err)
	}
	controlPool, err := db.OpenPool(ctx, controlDSN, 5*time.Second)
	if err != nil {
		t.Fatalf("open independent recovery control pool: %v", err)
	}
	t.Cleanup(controlPool.Close)
	store, err := controlstore.NewStore(ctx, controlPool)
	if err != nil {
		t.Fatalf("open recovery control store: %v", err)
	}
	opened, err := recovery.OpenInstance(ctx, store, recovery.OpenInstanceRequest{
		Kind: "recovery", OpenedBy: "deploy:drill-executor",
		Reason:                "local E2E: pre-effect restore gate refusal",
		EntryChains:           []uint64{31337},
		TargetGuardKey:        targetGuardKey,
		TargetRoleFingerprint: dataTarget.DataTargetFingerprint().RoleFingerprint,
	})
	if err != nil {
		t.Fatalf("open recovery instance with trusted entry-chain inventory: %v", err)
	}
	return controlDSN, opened.InstanceID
}

type e2eRecoveryDispatchDrop struct {
	URL     string
	dropped atomic.Bool
}

func e2eRecoveryDropDispatchProxy(t *testing.T, upstream string) *e2eRecoveryDispatchDrop {
	t.Helper()
	proxy := &e2eRecoveryDispatchDrop{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read json-rpc request", http.StatusBadRequest)
			return
		}
		var call struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(body, &call); err != nil {
			http.Error(w, "decode json-rpc request", http.StatusBadRequest)
			return
		}
		forward, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstream, bytes.NewReader(body))
		if err != nil {
			http.Error(w, "build upstream request", http.StatusBadGateway)
			return
		}
		forward.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(forward)
		if err != nil {
			http.Error(w, "real Anvil RPC unavailable: "+err.Error(), http.StatusBadGateway)
			return
		}
		if call.Method == "eth_sendRawTransaction" && proxy.dropped.CompareAndSwap(false, true) {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("HTTP test server does not support connection hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("drop dispatch response after real Anvil acceptance: hijack: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		defer response.Body.Close()
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	proxy.URL = server.URL
	t.Cleanup(server.Close)
	return proxy
}

func e2eRecoveryInternalDSN(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	u.Host = net.JoinHostPort("127.0.0.1", "5432")
	return u.String()
}

func setAnvilBalance(t *testing.T, client *rpc.Client, address common.Address, amount *big.Int) {
	t.Helper()
	var result any
	if err := client.CallContext(context.Background(), &result, "anvil_setBalance", address, hexutil.EncodeBig(amount)); err != nil {
		t.Fatalf("fund disposable Anvil signer: %v", err)
	}
}

func installRecoveryTransferEmitter(t *testing.T, client *rpc.Client, sender, recipient string, amount *big.Int) {
	t.Helper()
	topics := [3]common.Hash{
		crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)")),
		common.BytesToHash(common.HexToAddress(sender).Bytes()), common.BytesToHash(common.HexToAddress(recipient).Bytes()),
	}
	code := []byte{0x7f}
	code = append(code, common.BigToHash(amount).Bytes()...)
	code = append(code, 0x60, 0x00, 0x52)
	for _, topic := range [3]common.Hash{topics[2], topics[1], topics[0]} {
		code = append(code, 0x7f)
		code = append(code, topic.Bytes()...)
	}
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0xa3, 0x00)
	var result bool
	if err := client.CallContext(context.Background(), &result, "anvil_setCode", common.HexToAddress("0x2222222222222222222222222222222222222222"), hexutil.Encode(code)); err != nil {
		t.Fatalf("install disposable transfer-emitting code: %v", err)
	}
}

func dialRecoveryEth(t *testing.T, url string) *eth.Client {
	t.Helper()
	client, err := eth.Dial(context.Background(), url, 5*time.Second)
	if err != nil {
		t.Fatalf("dial Anvil eth client: %v", err)
	}
	return client
}

func recoveryRPC(t *testing.T, client *rpc.Client, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.CallContext(ctx, out, method, args...); err != nil {
		t.Fatalf("Anvil %s: %v", method, err)
	}
}

type e2eRecoveryBinding struct{ provider *nonce.ReadProvider }

func (b *e2eRecoveryBinding) ReadBinding(ctx context.Context, intentID string) (execution.BindingResult, error) {
	resp, err := b.provider.Read(ctx, nonce.ReadRequest{IntentID: intentID})
	if err != nil {
		return execution.BindingReadFailed, err
	}
	switch resp.Outcome {
	case nonce.ReadBound:
		if resp.Annotations == nil {
			return execution.BindingReadFailed, fmt.Errorf("008 bound read has no annotations")
		}
		a := resp.Annotations
		if a.Gate.State != nonce.ReadGateOpen || len(a.Gate.Causes) != 0 || a.Recovery.State != nonce.RecoveryNone || a.RegistryState != nonce.RegistryActive {
			return execution.BindingPaused, nil
		}
		return execution.BindingMatches, nil
	case nonce.ReadTerminal:
		return execution.BindingTerminal, nil
	case nonce.ReadNotBound:
		return execution.BindingAbsent, nil
	case nonce.ReadMismatch:
		return execution.BindingConflict, nil
	case nonce.ReadUnavailable:
		return execution.BindingReadFailed, nil
	default:
		return execution.BindingReadFailed, fmt.Errorf("unexpected 008 outcome %q", resp.Outcome)
	}
}
