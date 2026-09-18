//go:build integration

// joint_process_tracking_integration_test.go is Lane-F2: the continued
// receipt/confirmation/reorg-revision tracking after `completed`. It reuses
// the Lane-W process E2E shape (ordinary binary + real child process + the
// real Run loop; the test body never calls Driver/Advance/Reconcile/Claim/
// Allocate/PrepareAttempt) with a test-controlled confirmation policy and
// explicit chain control (anvil_mine / evm_snapshot / evm_revert).
//
// Contract under test (no semantic change):
//   - `completed` = the execution steps have converged (a `sent` fact can
//     trigger it) — it is NOT the payment success/verdict;
//   - after `completed`, the receipt/Transfer evidence, the canonicality
//     state, the confirmation depth and the reorg-revision tracking keep
//     running: in the SAME worker process (Test...AfterCompleted), after a
//     worker RESTART (Test...RestartTrackingAfterCompleted, startup
//     recovery) and across a real chain reorg (Test...ReorgAfterCompleted);
//   - tracking never re-allocates the intent/nonce, never grants a send
//     permission (a lost receipt, a reorg or a reconciliation outcome grant
//     nothing), never fakes a confirmation, and never blocks other objects;
//   - 008 allocation replay through the assembly path: the allocator is
//     idempotent per intent (allocated first, replayed on the identical
//     re-request), so repeated cycles keep exactly one binding and one
//     intent — asserted through the durable rows, never through SQL-preset
//     success states.
//
// Chain control (explicit, no background miner):
//   - Test...AfterCompleted keeps automine OFF so the broadcast stays pending
//     while the intent completes on the `sent` fact alone (the exact
//     "completed before depth" state this lane exists for); the test then
//     mines the inclusion and raises the height to the policy threshold.
//   - Test...RestartTrackingAfterCompleted restarts the worker process in
//     that same pre-inclusion state: the completed intent is no longer
//     claimable, so only startup recovery can carry its tracking.
//   - Test...ReorgAfterCompleted drives the REAL Anvil snapshot/revert
//     primitives: the mined payment disappears, the same signed bytes are
//     re-included at the new canonical height (identical tx hash, new block),
//     and 010's T035 revision pass must orphan the old canonical receipt and
//     reconfirm on the new one — never failure, never a re-send. The worker
//     is SIGSTOPped across the chain input so the reorg truth is fully
//     installed before any post-reorg probe can race it.
//
// The chain truth rows (chain_blocks) are the indexer's production duty and
// are controlled by the harness exactly like the joint acceptance's
// seedChainTruth does.
package txlifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// t28SyncChainTruth seeds the canonical chain-truth rows the confirmation
// tracker consumes (the indexer's production duty, controlled here by the
// harness exactly like the joint acceptance's seedChainTruth): every Anvil
// block up to head lands in chain_blocks. A reorg-era reset also clears stale
// canonical rows.
func t28SyncChainTruth(t *testing.T, j *jointEnv) {
	t.Helper()
	var head hexutil.Uint64
	jointRPC(t, &head, j.anvilURL, "eth_blockNumber")
	latest := uint64(head)
	j.mustExec(`DELETE FROM chain_blocks WHERE chain_id = $1`, jointChainID)
	for n := uint64(0); n <= latest; n++ {
		var blk struct {
			Hash   string `json:"hash"`
			Parent string `json:"parentHash"`
		}
		jointRPC(t, &blk, j.anvilURL, "eth_getBlockByNumber", fmt.Sprintf("0x%x", n), false)
		j.mustExec(`INSERT INTO chain_blocks (chain_id, number, hash, parent_hash, canonical)
			VALUES ($1,$2,$3,$4,TRUE)`,
			jointChainID, int64(n), strings.ToLower(blk.Hash), strings.ToLower(blk.Parent))
	}
}

// t28MineBlocks produces exactly n blocks (including any pending broadcast)
// and syncs the chain-truth rows: raising the chain height is the
// confirmation driver.
func t28MineBlocks(t *testing.T, j *jointEnv, n uint64) {
	t.Helper()
	var ignored json.RawMessage
	jointRPC(t, &ignored, j.anvilURL, "anvil_mine", hexutil.EncodeUint64(n))
	t28SyncChainTruth(t, j)
}

// t28SetAutomine toggles the node's automine so the test can hold a broadcast
// pending (completed-before-depth) or let it mine immediately (reorg scene).
func t28SetAutomine(t *testing.T, j *jointEnv, on bool) {
	t.Helper()
	var ok bool
	jointRPC(t, &ok, j.anvilURL, "evm_setAutomine", on)
	if ok != on {
		t.Fatalf("evm_setAutomine(%v) = %v", on, ok)
	}
}

// t28AttemptFacts reads the latest attempt's state plus its best-known receipt
// facts: the canonical receipt when one exists, else the newest receipt.
func (j *jointEnv) t28AttemptFacts(intentID string) (attemptState, canonicality string, confirmations int64) {
	j.t.Helper()
	if err := j.pool.QueryRow(j.ctx,
		`SELECT a.state,
		        COALESCE((SELECT r.canonicality FROM tx_receipts r
		                   WHERE r.attempt_id = a.attempt_id
		                   ORDER BY (r.canonicality = 'canonical') DESC, r.receipt_id DESC LIMIT 1), ''),
		        COALESCE((SELECT MAX(r.confirmations) FROM tx_receipts r
		                   WHERE r.attempt_id = a.attempt_id AND r.canonicality = 'canonical'), 0)
		   FROM tx_attempts a
		  WHERE a.intent_id = $1
		  ORDER BY a.created_at DESC, a.attempt_id DESC LIMIT 1`, intentID).
		Scan(&attemptState, &canonicality, &confirmations); err != nil {
		j.t.Fatalf("read attempt facts: %v", err)
	}
	return attemptState, canonicality, confirmations
}

// t28LatestAttemptID reads the intent's current (latest) attempt id.
func (j *jointEnv) t28LatestAttemptID(intentID string) string {
	j.t.Helper()
	var attemptID string
	if err := j.pool.QueryRow(j.ctx,
		`SELECT attempt_id FROM tx_attempts WHERE intent_id = $1
		  ORDER BY created_at DESC, attempt_id DESC LIMIT 1`, intentID).Scan(&attemptID); err != nil {
		j.t.Fatalf("read latest attempt id: %v", err)
	}
	return attemptID
}

// t28Count runs one integer count/scalar query.
func (j *jointEnv) t28Count(query string, args ...any) int {
	j.t.Helper()
	var n int
	if err := j.pool.QueryRow(j.ctx, query, args...).Scan(&n); err != nil {
		j.t.Fatalf("count query failed: %v", err)
	}
	return n
}

// t28EventCount counts the attempt's revision events of one kind.
func (j *jointEnv) t28EventCount(attemptID, event string) int {
	j.t.Helper()
	return j.t28Count(`SELECT count(*) FROM tx_attempt_events WHERE attempt_id = $1 AND event = $2`, attemptID, event)
}

// t28LogAttemptDump logs the durable revision facts + the worker output for a
// failing wait (diagnostic, never an assertion).
func t28LogAttemptDump(t *testing.T, s *laneWStack, intentID, attemptID string) {
	t.Helper()
	var state string
	var revision int64
	if err := s.j.pool.QueryRow(s.j.ctx, `SELECT state, revision_seq FROM tx_attempts WHERE attempt_id = $1`, attemptID).
		Scan(&state, &revision); err != nil {
		t.Logf("lane-f2 dump: read attempt: %v", err)
		return
	}
	t.Logf("lane-f2 dump: attempt=%s state=%s revision=%d events(orphaned=%d reconfirmed=%d) reconciliations=%d",
		attemptID, state, revision, s.j.t28EventCount(attemptID, "orphaned"), s.j.t28EventCount(attemptID, "reconfirmed"),
		s.j.t28Count(`SELECT count(*) FROM tx_reconciliations WHERE attempt_id = $1`, attemptID))
	rows, err := s.j.pool.Query(s.j.ctx,
		`SELECT canonicality, block_number, block_hash, confirmations, confirm_threshold,
		        COALESCE(confirmed_at::text, ''), COALESCE(orphaned_at::text, '')
		   FROM tx_receipts WHERE attempt_id = $1 ORDER BY receipt_id`, attemptID)
	if err != nil {
		t.Logf("lane-f2 dump: read receipts: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var canon, blockHash, confirmedAt, orphanedAt string
		var blockNumber, confirmations, threshold int64
		if err := rows.Scan(&canon, &blockNumber, &blockHash, &confirmations, &threshold, &confirmedAt, &orphanedAt); err != nil {
			t.Logf("lane-f2 dump: scan receipt: %v", err)
			return
		}
		t.Logf("lane-f2 dump: receipt canonicality=%s block=%d hash=%s conf=%d/%d confirmed_at=%s orphaned_at=%s",
			canon, blockNumber, blockHash, confirmations, threshold, confirmedAt, orphanedAt)
	}
	var intentState string
	_ = s.j.pool.QueryRow(s.j.ctx, `SELECT state FROM payment_intents WHERE intent_id = $1`, intentID).Scan(&intentState)
	var blocks int
	_ = s.j.pool.QueryRow(s.j.ctx, `SELECT count(*) FROM chain_blocks WHERE chain_id = $1`, jointChainID).Scan(&blocks)
	t.Logf("lane-f2 dump: intent=%s state=%s", intentID, intentState)
	t.Logf("lane-f2 dump: chain_blocks=%d\nworker stdout tail:\n%s\nworker stderr tail:\n%s",
		blocks, tailOf(s.out.String(), 2000), tailOf(s.errOut.String(), 2000))
}

// tailOf returns at most the last n bytes of s.
func tailOf(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// t28SignedBytes reads the persisted signed bytes + tx hash of one attempt
// (the chain-control material for the re-inclusion; never a 010/011 dispatch).
func (j *jointEnv) t28SignedBytes(attemptID string) ([]byte, string) {
	j.t.Helper()
	var raw []byte
	var hash string
	if err := j.pool.QueryRow(j.ctx,
		`SELECT signed_tx_bytes, tx_hash FROM tx_attempt_signings WHERE attempt_id = $1`, attemptID).
		Scan(&raw, &hash); err != nil {
		j.t.Fatalf("read signed bytes: %v", err)
	}
	return raw, hash
}

// t28WaitIntent polls the durable intent state until want or the bound; each
// poll first syncs the chain-truth rows (the indexer's continuous duty,
// harness-controlled like seedChainTruth). The failure dump carries the
// worker output and the durable facts.
func t28WaitIntent(t *testing.T, pool *pgxpool.Pool, j *jointEnv, intentID, want string, stdout, stderr *bytes.Buffer, bound time.Duration) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		t28SyncChainTruth(t, j)
		var state string
		err := pool.QueryRow(context.Background(),
			`SELECT state FROM payment_intents WHERE intent_id = $1`, intentID).Scan(&state)
		if err == nil && state == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent %s never reached %s within the bound (last=%s err=%v)\nworker stdout:\n%s\nworker stderr:\n%s",
				intentID, want, state, err, stdout.String(), stderr.String())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// t28WaitUntil polls until cond is true or the bound expires.
func t28WaitUntil(t *testing.T, bound time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("bounded wait expired: %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// laneWStack is the disposable stack for the process tests: real migrated PG,
// the harness Anvil, the in-process real 009 signer-serve, and the built
// binary with the real child-process handle. The test writes its own
// confirmation-threshold policy row (test-controlled depth, the real
// production source table) and controls the chain explicitly.
type laneWStack struct {
	j      *jointEnv
	binary string
	cmd    *exec.Cmd
	cancel context.CancelFunc
	done   chan error
	out    *bytes.Buffer
	errOut *bytes.Buffer
}

func newLaneWStack(t *testing.T, threshold int64) *laneWStack {
	t.Helper()
	j := newJointEnv(t)
	j.store = NewStore(j.pool).WithChain(j.eth, 15*time.Second).WithSigner(j.signer)
	j.mustExec(`INSERT INTO confirmation_policy_history (chain_id, policy_seq, threshold)
		VALUES ($1, 1, $2) ON CONFLICT (chain_id, policy_seq) DO UPDATE SET threshold = EXCLUDED.threshold`,
		jointChainID, threshold)
	t28SyncChainTruth(t, j)
	s := &laneWStack{j: j, binary: buildWorkerBinary(t)}
	s.startWorker(t)
	t.Cleanup(func() { s.stopWorker(t) })
	return s
}

// startWorker launches the ordinary binary as a REAL child process running
// `txharbor withdrawal-worker` (fresh buffers per start: a restart's
// wiring-ready line is detectable again).
func (s *laneWStack) startWorker(t *testing.T) {
	t.Helper()
	s.out, s.errOut = &bytes.Buffer{}, &bytes.Buffer{}
	pctx, pcancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(pctx, s.binary, "withdrawal-worker")
	cmd.Env = workerProcessEnv(s.j.dsn, s.j.anvilURL, s.j.signerURL, s.j.signerCredential, "lane-f2")
	cmd.Stdout = s.out
	cmd.Stderr = s.errOut
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		pcancel()
		t.Fatalf("start worker process: %v", err)
	}
	s.cmd, s.cancel, s.done = cmd, pcancel, make(chan error, 1)
	go func() { s.done <- cmd.Wait() }()
}

// stopWorker cancels the process context with a bounded kill fallback; a
// paused process is resumed first so the kill path is reachable.
func (s *laneWStack) stopWorker(t *testing.T) {
	t.Helper()
	if s.cancel == nil {
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGCONT)
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.done
	}
	s.cancel = nil
}

// pauseWorker SIGSTOPs the child for test-controlled chain surgery; cleanup
// always resumes so a failed test cannot leave a stopped process behind.
func (s *laneWStack) pauseWorker(t *testing.T) {
	t.Helper()
	if err := s.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP worker: %v", err)
	}
	t.Cleanup(func() { _ = s.cmd.Process.Signal(syscall.SIGCONT) })
}

// resumeWorker SIGCONTs the paused child.
func (s *laneWStack) resumeWorker(t *testing.T) {
	t.Helper()
	if err := s.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT worker: %v", err)
	}
}

// waitWiringReady waits for the real startup line (the Run loop is entered).
func (s *laneWStack) waitWiringReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if strings.Contains(s.out.String(), "joint wiring ready") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker process never printed the wiring-ready line\nstdout:\n%s\nstderr:\n%s",
				s.out.String(), s.errOut.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// laneWAdmit drives the controlled request creation and authorization supply
// through real HTTP only (007 create + 011 admission), leaving claim/binding/
// attempt to the worker process.
func laneWAdmit(t *testing.T, j *jointEnv) *jointIntent {
	j.seq++
	n := j.seq
	authID := fmt.Sprintf("wa-f2-%d", n)
	intentID := fmt.Sprintf("intent-f2-%d", n)

	j.mustExec(`INSERT INTO deposit_config_history (chain_id, version_seq, config_hash, start_block, assets, watches, replay_from, request_id)
		VALUES ($1,$2,$3,0,$4,$5,0,$6) ON CONFLICT DO NOTHING`,
		jointChainID, n, strings.Repeat("a", 64), j.asset+":0", j.recipient+":0", fmt.Sprintf("cfg-f2-%d", n))
	if _, err := withdrawal.SupplyGrant(j.ctx, j.pool, withdrawal.OpInput{
		OperationID: fmt.Sprintf("op-f2-%d", n), Action: "supply",
		AuthorizationID: authID, CallerID: j.callerID, ChainID: jointChainID,
		Asset: j.asset, Recipient: j.recipient, Amount: j.amount,
	}, "lane-f2", "controlled authorization supply"); err != nil {
		t.Fatalf("SupplyGrant: %v", err)
	}
	if _, err := j.pool.Exec(j.ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'lane-f2') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, j.callerID); err != nil {
		t.Fatalf("seed permission: %v", err)
	}
	j.mustExec(`INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq) VALUES ($1,$2,'active',1)
		ON CONFLICT (chain_id, sender) DO UPDATE SET state='active'`, jointChainID, j.sender)
	j.mustExec(`INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1,$2)
		ON CONFLICT (chain_id, sender) DO NOTHING`, jointChainID, j.sender)

	createSrv := httptest.NewServer(&app.WithdrawalHandler{Pool: j.pool, ChainID: jointChainID})
	defer createSrv.Close()
	body, _ := json.Marshal(map[string]any{
		"idempotency_key": fmt.Sprintf("idem-f2-%d", n), "chain_id": jointChainID,
		"asset": j.asset, "recipient": j.recipient, "amount": j.amount, "authorization_id": authID,
	})
	status, raw := jointHTTPDo(t, http.MethodPost, createSrv.URL+"/withdrawals", j.apiKey, string(body))
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("007 create status=%d body=%s", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.RequestID == "" {
		t.Fatalf("007 create body=%s err=%v", raw, err)
	}
	signingRequestID := created.RequestID

	j.mustExec(`INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE,1,'lane-f2')`,
		authID, intentID, signingRequestID, j.sender, int64(1e15), int64(2e9), int64(15e8))

	admitSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux := http.NewServeMux()
		mux.Handle("POST /withdrawals/{request_id}/execution",
			&app.WithdrawalExecutionHandler{Pool: j.pool, ChainID: jointChainID})
		mux.ServeHTTP(w, r)
	}))
	defer admitSrv.Close()
	status, raw = jointHTTPDo(t, http.MethodPost, admitSrv.URL+"/withdrawals/"+created.RequestID+"/execution", j.apiKey, "")
	if status != http.StatusCreated {
		t.Fatalf("011 admit status=%d body=%s", status, raw)
	}
	var admitted struct {
		IntentID string `json:"intent_id"`
	}
	if err := json.Unmarshal(raw, &admitted); err != nil || admitted.IntentID != intentID {
		t.Fatalf("011 admit body=%s err=%v", raw, err)
	}
	return &jointIntent{
		requestID: created.RequestID, intentID: intentID, authorizationID: authID,
		signingRequestID: signingRequestID,
	}
}

// t28AssertNoReplay asserts the durable one-binding/one-attempt/one-send/
// one-intent identity the 008 replay through the assembly path must keep.
func t28AssertNoReplay(t *testing.T, j *jointEnv, jj *jointIntent) {
	t.Helper()
	if n := j.t28Count(`SELECT count(*) FROM nonce_bindings WHERE intent_id = $1`, jj.intentID); n != 1 {
		t.Fatalf("bindings for the intent = %d, want exactly 1 (replayed allocation, never a second nonce)", n)
	}
	if n := j.t28Count(`SELECT count(*) FROM tx_attempts WHERE intent_id = $1`, jj.intentID); n != 1 {
		t.Fatalf("attempts for the intent = %d, want exactly 1 (never a second attempt)", n)
	}
	if n := j.t28Count(`SELECT count(*) FROM tx_send_attempts sa JOIN tx_attempts a ON a.attempt_id = sa.attempt_id
		WHERE a.intent_id = $1`, jj.intentID); n != 1 {
		t.Fatalf("dispatch rows for the intent = %d, want exactly 1 (tracking never re-sends)", n)
	}
	if n := j.t28Count(`SELECT count(*) FROM payment_intents WHERE request_id = $1`, jj.requestID); n != 1 {
		t.Fatalf("intents for the request = %d, want exactly 1", n)
	}
	var intentState string
	if err := j.pool.QueryRow(j.ctx, `SELECT state FROM payment_intents WHERE intent_id = $1`, jj.intentID).Scan(&intentState); err != nil {
		t.Fatalf("read intent state: %v", err)
	}
	if intentState != "completed" {
		t.Fatalf("intent state = %s, want completed (completed != confirmed display)", intentState)
	}
}

// TestJointProcessTrackingAfterCompleted pins the in-process core of Lane-F2:
// the broadcast is held pending (automine off), so the intent reaches
// `completed` on the `sent` fact alone — no receipt, no depth. The SAME
// worker process (no restart) must then file the receipt and walk the
// confirmation depth to the policy threshold for the already-completed
// intent, without re-sending, re-allocating or faking a confirmation.
func TestJointProcessTrackingAfterCompleted(t *testing.T) {
	const threshold = int64(3)
	s := newLaneWStack(t, threshold)
	s.waitWiringReady(t)
	s.j.installTransferEmit(1000)

	t28SetAutomine(t, s.j, false)
	jj := laneWAdmit(t, s.j)

	t28WaitIntent(t, s.j.pool, s.j, jj.intentID, "completed", s.out, s.errOut, 90*time.Second)
	state, _, conf := s.j.t28AttemptFacts(jj.intentID)
	receipts := s.j.t28Count(`SELECT count(*) FROM tx_receipts r JOIN tx_attempts a ON a.attempt_id = r.attempt_id
		WHERE a.intent_id = $1`, jj.intentID)
	if state != "sent" || conf != 0 || receipts != 0 {
		t.Fatalf("post-completion pre-inclusion facts = (state=%s confirmations=%d receipts=%d), want (sent, 0, 0)", state, conf, receipts)
	}
	t.Logf("lane-f2: completed on the sent fact alone (attempt=%s receipts=%d); depth tracking must continue in-process", state, receipts)

	// Inclusion, then the height to the threshold: only the worker's tracking
	// can move the completed intent's receipt facts.
	t28MineBlocks(t, s.j, 1)
	t28WaitUntil(t, 60*time.Second, "the receipt filed after completed", func() bool {
		_, canon, _ := s.j.t28AttemptFacts(jj.intentID)
		return canon == "canonical"
	})
	t28MineBlocks(t, s.j, uint64(threshold-1))
	t28WaitUntil(t, 60*time.Second, "the attempt confirmed at the policy threshold", func() bool {
		st, canon, c := s.j.t28AttemptFacts(jj.intentID)
		return st == "confirmed" && canon == "canonical" && c >= threshold
	})

	state, canon, conf := s.j.t28AttemptFacts(jj.intentID)
	if state != "confirmed" || canon != "canonical" || conf < threshold {
		t.Fatalf("attempt facts = (%s %s %d), want (confirmed canonical >=%d)", state, canon, conf, threshold)
	}
	t.Logf("lane-f2: depth tracking reached in-process (%s/%s/%d)", state, canon, conf)
	t28AssertNoReplay(t, s.j, jj)
}

// TestJointProcessRestartTrackingAfterCompleted pins startup recovery: the
// worker is restarted while the completed intent is still pre-inclusion (its
// attempt `sent`, no receipt). A completed intent is never claimable again,
// so only the restart's tracking pass can carry its receipt/confirmation
// tracking; the fresh process must reach the confirmation condition.
func TestJointProcessRestartTrackingAfterCompleted(t *testing.T) {
	const threshold = int64(2)
	s := newLaneWStack(t, threshold)
	s.waitWiringReady(t)
	s.j.installTransferEmit(1000)

	t28SetAutomine(t, s.j, false)
	jj := laneWAdmit(t, s.j)
	t28WaitIntent(t, s.j.pool, s.j, jj.intentID, "completed", s.out, s.errOut, 90*time.Second)
	if state, _, _ := s.j.t28AttemptFacts(jj.intentID); state != "sent" {
		t.Fatalf("pre-restart attempt state = %s, want sent (completed before depth)", state)
	}
	if n := s.j.t28Count(`SELECT count(*) FROM tx_receipts r JOIN tx_attempts a ON a.attempt_id = r.attempt_id
		WHERE a.intent_id = $1`, jj.intentID); n != 0 {
		t.Fatalf("pre-restart receipts = %d, want 0", n)
	}

	// Restart: a fresh process rebuilds its tracking set from PostgreSQL.
	s.stopWorker(t)
	s.startWorker(t)
	s.waitWiringReady(t)

	t28MineBlocks(t, s.j, 1)
	t28WaitUntil(t, 60*time.Second, "the restarted process files the receipt after completed", func() bool {
		_, canon, _ := s.j.t28AttemptFacts(jj.intentID)
		return canon == "canonical"
	})
	t28MineBlocks(t, s.j, uint64(threshold-1))
	t28WaitUntil(t, 60*time.Second, "the restarted process confirms at the policy threshold", func() bool {
		st, canon, c := s.j.t28AttemptFacts(jj.intentID)
		return st == "confirmed" && canon == "canonical" && c >= threshold
	})
	t28AssertNoReplay(t, s.j, jj)
}

// TestJointProcessReorgAfterCompleted pins reorg/revision tracking after
// completion with the real chain primitives: revert the payment away, then
// re-include the SAME signed bytes at the new canonical height. 010's T035
// pass must revise the old canonical receipt to orphaned and then reconfirm
// on the new canonical block — for an intent that already is `completed`,
// through the same worker process, with no new attempt, send or nonce.
func TestJointProcessReorgAfterCompleted(t *testing.T) {
	const threshold = int64(1)
	s := newLaneWStack(t, threshold)
	s.waitWiringReady(t)
	s.j.installTransferEmit(1000)

	// Snapshot BEFORE the payment exists, so the revert removes exactly the
	// mined payment transaction (a real chain reorg under the receipt).
	var snapshot hexutil.Uint64
	jointRPC(t, &snapshot, s.j.anvilURL, "evm_snapshot")

	jj := laneWAdmit(t, s.j)
	t28WaitIntent(t, s.j.pool, s.j, jj.intentID, "completed", s.out, s.errOut, 90*time.Second)
	t28WaitUntil(t, 60*time.Second, "the attempt confirmed before the reorg", func() bool {
		st, canon, c := s.j.t28AttemptFacts(jj.intentID)
		return st == "confirmed" && canon == "canonical" && c >= threshold
	})
	attemptID := s.j.t28LatestAttemptID(jj.intentID)

	// Freeze the worker so the reorg input is fully installed in the chain
	// truth before any post-reorg probe can race it.
	s.pauseWorker(t)
	jointRevert(t, s.j.anvilURL, snapshot)
	t28SyncChainTruth(t, s.j)

	// Re-include the SAME signed bytes: identical tx hash, new canonical
	// block. This is chain control (the reorg's re-inclusion), never a
	// 010/011 dispatch: no send row can result from it.
	raw, storedHash := s.j.t28SignedBytes(attemptID)
	var newHash string
	jointRPC(t, &newHash, s.j.anvilURL, "eth_sendRawTransaction", hexutil.Encode(raw))
	if !strings.EqualFold(newHash, storedHash) {
		t.Fatalf("re-included tx hash = %s, want the identical signed-bytes hash %s", newHash, storedHash)
	}
	// Force the inclusion block (automine can return before the block lands
	// right after a snapshot revert) and only then read the chain truth, so
	// the worker's post-reorg probe sees the new block as canonical.
	t28MineBlocks(t, s.j, 1)
	s.resumeWorker(t)

	// The same worker process keeps tracking: the old canonical receipt is
	// revised to orphaned (history retained), then the attempt reconfirms on
	// the new canonical block — never failure, never a re-send permission.
	t28WaitUntil(t, 90*time.Second, "the receipt revised to orphaned after the reorg", func() bool {
		return s.j.t28EventCount(attemptID, "orphaned") >= 1
	})
	reconfirmDeadline := time.Now().Add(90 * time.Second)
	for s.j.t28EventCount(attemptID, "reconfirmed") < 1 {
		if time.Now().After(reconfirmDeadline) {
			t28LogAttemptDump(t, s, jj.intentID, attemptID)
			t.Fatalf("bounded wait expired: the attempt reconfirmed on the new canonical block")
		}
		time.Sleep(500 * time.Millisecond)
	}

	if n := s.j.t28Count(`SELECT count(*) FROM tx_receipts WHERE attempt_id = $1`, attemptID); n != 2 {
		t.Fatalf("receipt rows after the reorg = %d, want 2 (one orphaned + one canonical)", n)
	}
	if n := s.j.t28Count(`SELECT count(*) FROM tx_receipts WHERE attempt_id = $1 AND canonicality = 'orphaned'`, attemptID); n != 1 {
		t.Fatalf("orphaned receipt rows = %d, want 1", n)
	}
	if n := s.j.t28Count(`SELECT count(*) FROM tx_receipts WHERE attempt_id = $1 AND canonicality = 'canonical'`, attemptID); n != 1 {
		t.Fatalf("canonical receipt rows = %d, want 1", n)
	}
	state, canon, conf := s.j.t28AttemptFacts(jj.intentID)
	if state != "confirmed" || canon != "canonical" || conf < threshold {
		t.Fatalf("attempt facts after the reorg = (%s %s %d), want (confirmed canonical >=%d)", state, canon, conf, threshold)
	}
	// The reorg revision is tracked evidence, never a rebuilt payment.
	t28AssertNoReplay(t, s.j, jj)
	if n := s.j.t28Count(`SELECT count(*) FROM tx_reconciliations WHERE attempt_id = $1`, attemptID); n < 2 {
		t.Fatalf("reconciliation observations after the reorg = %d, want at least 2", n)
	}
}

// jointRevert rewinds the chain to a previously taken snapshot (real reorg).
func jointRevert(t *testing.T, url string, id hexutil.Uint64) {
	t.Helper()
	var ok bool
	jointRPC(t, &ok, url, "evm_revert", hexutil.Uint64(id))
	if !ok {
		t.Fatalf("evm_revert(%d) = false", id)
	}
}
