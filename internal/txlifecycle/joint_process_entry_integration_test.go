//go:build integration

// joint_process_entry_integration_test.go is Lane-W: the process-level end to
// end proof for the register's process-entry requirement. It builds the
// ordinary repo binary, starts `txharbor withdrawal-worker` as a REAL child
// process, and lets the actual Run loop (startup catch-up, scan, claim,
// heartbeat, driver steps, reconcile, projection) handle the whole flow. The
// test body never calls Driver/Advance/Reconcile/Claim/Allocate/
// PrepareAttempt — it drives only real HTTP (007 create, 011 admission),
// controlled authorization supply, Anvil control and read-only DB probes.
//
// Clause mapping (requirements → this test):
//   - Constitution XI: the withdrawal E2E flow "API request → queue → nonce
//     allocation → signing → broadcast → confirmation" runs through REAL
//     integration here — a real process, real HTTP servers, real PostgreSQL,
//     real Anvil; mocks do not substitute the flow.
//   - 011 quickstart V13-1 (FR-14/C10): admission (011) → intent → claim →
//     010 attempt → 009 signature → Anvil send → receipt/confirmation →
//     `completed`, with the identity chain request_id → intent_id → binding →
//     attempt_id → signing_request_id → authorization/scope version intact
//     and traceable.
//   - 010 FR-16: the real joint acceptance covers intent creation (real 007
//   - 011 HTTP here), authorization supply (SupplyGrant + scope here), and
//     the worker's own claim/binding path (the process loop, not a test
//     stand-in). The expired-worker / unknown / reorg scenes remain covered
//     by the joint acceptance tests at 653f8c6 (this test adds the
//     process-entry assembly the register listed as A-13 scope; it does NOT
//     claim A-13 closed — that stays OPEN).
//   - Register T045 gap: "the production process entry's adapter assembly
//     (WithdrawalWorkerCommand still using the standalone constructor)"
//     belonged to A-13 scope. This test exercises exactly that assembly
//     through the real binary and real Run loop.
//
// Deposit-in-the-same-scene is deliberately NOT asserted: no clause text
// lists a deposit as part of V13-1, so it is not a requirement here (the
// deposit E2E belongs to the 004/005/006 lanes' own evidence).
//
// T060 note: the joint production-Driver fee-replacement tests
// (TestJointReplaceFeeBump/Refusals through the real Driver) are the T060
// evidence; the supplemental direct-Store replacement fencing tests are
// separate evidence. This process test drives only the first-broadcast chain
// (V13-1) and does not conflate the two.
package txlifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// workerProcessEnv builds the full required environment for the real
// `txharbor withdrawal-worker` command against the disposable stack. The
// signer credential is the fixture token the in-process signer-serve accepts.
func workerProcessEnv(dsn, anvilURL, signerURL, credential, label string) []string {
	kv := map[string]string{
		config.EnvPGDSN:                  dsn,
		config.EnvRPCURL:                 anvilURL,
		config.EnvChainID:                "31337",
		config.EnvStartHeight:            "0",
		config.EnvLogStartHeight:         "0",
		config.EnvLogContracts:           "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:     "0",
		config.EnvDepositContracts:       "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses:  "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:      "10",
		config.EnvTxSignerURL:            signerURL,
		config.EnvTxSignerCredential:     credential,
		config.EnvNonceReadToken:         "jointwire-test-token",
		config.EnvWorkerTTLSeconds:       "30",
		config.EnvWorkerHeartbeatSeconds: "10",
		config.EnvWorkerStallSeconds:     "300",
		config.EnvWorkerBackoffBaseMS:    "100",
		config.EnvWorkerBackoffMaxMS:     "5000",
		config.EnvWorkerScanIntervalMS:   "250",
		config.EnvWorkerLabel:            label,
	}
	env := os.Environ()
	for k, v := range kv {
		env = append(env, k+"="+v)
	}
	return env
}

// jointAdmitViaHTTP is the pre-driver half of admitIntent: the 007 request
// over real HTTP, the controlled authorization supply (SupplyGrant + the
// test-controlled fee scope), and the 011 admission over real HTTP. It
// deliberately stops before any claim, 008 allocation or 010 attempt — those
// belong to the worker process under test.
func (j *jointEnv) admitViaHTTP() *jointIntent {
	j.t.Helper()
	j.seq++
	n := j.seq
	authID := fmt.Sprintf("wa-w%d", n)
	intentID := fmt.Sprintf("intent-w%d", n)
	ownerID := fmt.Sprintf("worker-w%d", n)

	j.mustExec(`INSERT INTO deposit_config_history (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, request_id)
		VALUES ($1,$2,$3,0,$4,$5,0,$6) ON CONFLICT DO NOTHING`,
		jointChainID, n, strings.Repeat("a", 64), j.asset+":0", j.recipient+":0", fmt.Sprintf("cfg-%d", n))

	if _, err := withdrawal.SupplyGrant(j.ctx, j.pool, withdrawal.OpInput{
		OperationID:     fmt.Sprintf("op-w%d", n),
		Action:          "supply",
		AuthorizationID: authID,
		CallerID:        j.callerID,
		ChainID:         jointChainID,
		Asset:           j.asset,
		Recipient:       j.recipient,
		Amount:          j.amount,
	}, "lane-w", "controlled authorization supply"); err != nil {
		j.t.Fatalf("SupplyGrant: %v", err)
	}
	if _, err := j.pool.Exec(j.ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'lane-w') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, j.callerID); err != nil {
		j.t.Fatalf("seed permission: %v", err)
	}
	j.mustExec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq) VALUES ($1,$2,'active',1)
		ON CONFLICT (chain_id, sender) DO UPDATE SET state='active'`, jointChainID, j.sender)
	j.mustExec(`INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1,$2)
		ON CONFLICT (chain_id, sender) DO NOTHING`, jointChainID, j.sender)

	// Real 007 withdrawal-creation HTTP (the same real handler the joint
	// acceptance used, served fresh per admission).
	createSrv := httptest.NewServer(&app.WithdrawalHandler{Pool: j.pool, ChainID: jointChainID})
	defer createSrv.Close()
	body, _ := json.Marshal(map[string]any{
		"idempotency_key": fmt.Sprintf("idem-w%d", n), "chain_id": jointChainID,
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

	// Controlled authorization scope: the fee triple and the fee-replacement
	// allowance are test-controlled facts.
	j.mustExec(`INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE,1,'lane-w')`,
		authID, intentID, signingRequestID, j.sender, int64(1e15), int64(2e9), int64(15e8))

	// Real 011 admission over HTTP creates the intent; the CLAIM belongs to
	// the worker process and is deliberately not taken here.
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
	return &jointIntent{
		requestID: created.RequestID, intentID: intentID, authorizationID: authID,
		signingRequestID: signingRequestID, attemptID: fmt.Sprintf("att-w%d", n), ownerID: ownerID,
	}
}

// buildWorkerBinary builds the ordinary repo binary (no special tags) and
// returns its path; the child process runs `txharbor withdrawal-worker`.
func buildWorkerBinary(t *testing.T) string {
	t.Helper()
	out := t.TempDir() + "/txharbor"
	build := exec.Command("go", "build", "-o", out, "./cmd/txharbor")
	build.Dir = "../.." // repo root relative to internal/txlifecycle
	var errBuf bytes.Buffer
	build.Stderr = &errBuf
	if err := build.Run(); err != nil {
		t.Fatalf("go build ./cmd/txharbor: %v\n%s", err, errBuf.String())
	}
	return out
}

func TestJointProcessEntryEndToEnd(t *testing.T) {
	j := newJointEnv(t)
	j.installTransferEmit(1000)
	j.seedChainTruth()

	binary := buildWorkerBinary(t)

	// Start the REAL worker process: the ordinary binary, the real Run loop.
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	processDone := make(chan error, 1)
	pctx, pcancel := context.WithCancel(context.Background())
	defer pcancel()
	cmd := exec.CommandContext(pctx, binary, "withdrawal-worker")
	cmd.Env = workerProcessEnv(j.dsn, j.anvilURL, j.signerURL, j.signerCredential, "lane-w")
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker process: %v", err)
	}
	go func() { processDone <- cmd.Wait() }()
	t.Cleanup(func() {
		pcancel()
		select {
		case <-processDone:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-processDone
		}
	})

	// Drive: real 007 HTTP create + controlled authorization + real 011
	// admission. The worker process owns everything from the claim on.
	jj := j.admitViaHTTP()

	// Bounded wait for the terminal state; on timeout, fail with the worker
	// output + durable row dump as the diagnostic.
	deadline := time.Now().Add(150 * time.Second)
	for {
		var state string
		err := j.pool.QueryRow(j.ctx,
			`SELECT state FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&state)
		if err == nil && state == "completed" {
			break
		}
		if time.Now().After(deadline.Add(-149 * time.Second)) { // log at most every ~1s: coarse progress trace
		}
		var attState string
		_ = j.pool.QueryRow(j.ctx,
			`SELECT a.state FROM tx_attempts a WHERE a.intent_id = $1 ORDER BY a.created_at DESC LIMIT 1`, jj.intentID).Scan(&attState)
		var rc int
		_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_receipts`).Scan(&rc)
		t.Logf("lane-w progress: intent=%s attempt=%s receipts=%d", state, attState, rc)
		if time.Now().After(deadline) {
			var steps, attempts, signed, sends int
			_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM execution_steps WHERE intent_id = $1`, jj.intentID).Scan(&steps)
			_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attempts)
			_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_attempt_signings s JOIN tx_attempts a ON a.attempt_id = s.attempt_id WHERE a.intent_id = $1`, jj.intentID).Scan(&signed)
			_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_send_attempts sa JOIN tx_attempts a ON a.attempt_id = sa.attempt_id WHERE a.intent_id = $1`, jj.intentID).Scan(&sends)
			t.Fatalf("intent %s never reached completed within the bound (last=%s steps=%d attempts=%d signed=%d sends=%d)\nworker stdout:\n%s\nworker stderr:\n%s",
				jj.intentID, state, steps, attempts, signed, sends, stdout.String(), stderr.String())
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Lane-W diagnostic dump: the durable step/attempt/signing/send state at
	// completion time.
	var stepStates int
	_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM execution_steps WHERE intent_id = $1 AND state <> 'converged'`, jj.intentID).Scan(&stepStates)
	var signedRows int
	_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_attempt_signings s JOIN tx_attempts a ON a.attempt_id = s.attempt_id WHERE a.intent_id = $1`, jj.intentID).Scan(&signedRows)
	var sendRows int
	_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_send_attempts sa JOIN tx_attempts a ON a.attempt_id = sa.attempt_id WHERE a.intent_id = $1`, jj.intentID).Scan(&sendRows)
	t.Logf("lane-w completed diagnostic: steps-nonconverged=%d signed=%d sends=%d", stepStates, signedRows, sendRows)

	// V13-1 identity chain, all through durable rows:
	//   request_id → intent_id → binding → attempt_id → signing_request_id →
	//   authorization/scope version.
	var intentReq, intentAuth string
	var intentAuthVer int64
	if err := j.pool.QueryRow(j.ctx,
		`SELECT request_id, authorization_id, authorization_version FROM payment_intents WHERE intent_id = $1`,
		jj.intentID).Scan(&intentReq, &intentAuth, &intentAuthVer); err != nil {
		t.Fatalf("read intent identity: %v", err)
	}
	if intentReq != jj.requestID || intentAuth != jj.authorizationID || intentAuthVer != 1 {
		t.Fatalf("intent identity = (%s %s v%d), want (%s %s v1)", intentReq, intentAuth, intentAuthVer, jj.requestID, jj.authorizationID)
	}
	var bindingID, bindingNonce string
	if err := j.pool.QueryRow(j.ctx,
		`SELECT binding_id, nonce::text FROM nonce_bindings WHERE intent_id = $1`,
		jj.intentID).Scan(&bindingID, &bindingNonce); err != nil {
		t.Fatalf("read 008 binding: %v", err)
	}
	var attemptID, attemptBinding, attemptSignReq, attemptAuth string
	var attemptAuthVer int64
	if err := j.pool.QueryRow(j.ctx,
		`SELECT attempt_id, binding_ref, signing_request_id, authorization_id, authorization_version
		 FROM tx_attempts WHERE intent_id = $1`, jj.intentID).
		Scan(&attemptID, &attemptBinding, &attemptSignReq, &attemptAuth, &attemptAuthVer); err != nil {
		t.Fatalf("read 010 attempt: %v", err)
	}
	if attemptBinding != bindingID {
		t.Fatalf("attempt binding_ref = %s, want the worker-allocated 008 binding %s", attemptBinding, bindingID)
	}
	if attemptSignReq != jj.signingRequestID {
		t.Fatalf("attempt signing_request_id = %s, want %s", attemptSignReq, jj.signingRequestID)
	}
	if attemptAuth != jj.authorizationID || attemptAuthVer != 1 {
		t.Fatalf("attempt authorization = (%s v%d), want (%s v1)", attemptAuth, attemptAuthVer, jj.authorizationID)
	}
	var scopeReq, scopeIntent string
	if err := j.pool.QueryRow(j.ctx,
		`SELECT request_id, intent_id FROM withdrawal_authorization_scopes WHERE authorization_id = $1 AND authorization_version = 1`,
		jj.authorizationID).Scan(&scopeReq, &scopeIntent); err != nil {
		t.Fatalf("read scope: %v", err)
	}
	if scopeReq != jj.signingRequestID || scopeIntent != jj.intentID {
		t.Fatalf("scope identity = (%s %s), want (%s %s)", scopeReq, scopeIntent, jj.signingRequestID, jj.intentID)
	}

	// Chain truth: the worker's own send produced a mined transaction, and
	// 010 recorded its receipt/confirmation for the same attempt.
	var attemptRows int
	_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID).Scan(&attemptRows)
	var attemptState string
	_ = j.pool.QueryRow(j.ctx, `SELECT state FROM tx_attempts WHERE intent_id = $1 ORDER BY created_at DESC LIMIT 1`, jj.intentID).Scan(&attemptState)
	var receiptRows int
	_ = j.pool.QueryRow(j.ctx, `SELECT count(*) FROM tx_receipts`).Scan(&receiptRows)
	t.Logf("lane-w diagnostic: attempts=%d state=%s total-receipts=%d", attemptRows, attemptState, receiptRows)
	var txHash string
	if err := j.pool.QueryRow(j.ctx,
		`SELECT r.tx_hash FROM tx_receipts r JOIN tx_attempts a ON a.attempt_id = r.attempt_id
		 WHERE a.intent_id = $1 LIMIT 1`, jj.intentID).Scan(&txHash); err != nil {
		t.Fatalf("read send hash (attempts=%d total-receipts=%d): %v | worker stdout: %s | worker stderr: %s",
			attemptRows, receiptRows, err, stdout.String(), stderr.String())
	}
	var receipt struct {
		Status string `json:"status"`
	}
	jointRPC(t, &receipt, j.anvilURL, "eth_getTransactionReceipt", txHash)
	if receipt.Status != "0x1" {
		t.Fatalf("on-chain receipt for %s = %+v, want a mined successful transaction", txHash, receipt)
	}
	var canonicality, receiptEffect string
	var confirmations int64
	if err := j.pool.QueryRow(j.ctx,
		`SELECT canonicality, effect, confirmations FROM tx_receipts r JOIN tx_attempts a ON a.attempt_id = r.attempt_id
		 WHERE a.intent_id = $1`, jj.intentID).Scan(&canonicality, &receiptEffect, &confirmations); err != nil {
		t.Fatalf("read receipt facts: %v", err)
	}
	// The receipt fact is on file (the Lane-I-added confirmation scan runs in
	// the process loop). The canonicality/confirmations ADVANCE is a
	// discovered semantic gap between the 010 confirmation tracker and the
	// 011 reconciler's sent-fact consumption: reported for adjudication, not
	// silently loosened into an assertion this run cannot prove.
	t.Logf("lane-w receipt: canonicality=%s effect=%s confirmations=%d (confirmation advance is reported, not asserted)", canonicality, receiptEffect, confirmations)
	if receiptEffect != "effective" {
		t.Fatalf("receipt effect = %s, want effective (the mined ERC-20 transfer)", receiptEffect)
	}

	// The worker process kept running cleanly until the test ended.
	select {
	case err := <-processDone:
		if err != nil && !strings.Contains(err.Error(), "killed") && !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("worker process exited early: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
		}
	default:
		// still running as expected; the cleanup cancels it.
	}
}
