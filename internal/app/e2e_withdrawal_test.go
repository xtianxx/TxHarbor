//go:build e2e

// e2e_withdrawal_test.go is the T079 core withdrawal E2E: the real 007 API
// request, the persisted receipt and its same-transaction event, the real 011
// admission and state-machine transitions with their emitted events, and the
// real publisher/reference consumer round trip over a real Kafka broker.
// A duplicate request creates no second request and no second event, and the
// withdrawal.request.received event never changes the Accepted semantics.
//
// The 010/008/009 broadcast leg (signer + nonce + chain send) is the upstream
// lanes' own E2E scope; this test drives the 011 domain transitions the
// worker would drive, through the same public functions, and asserts that no
// event path reaches a send action.
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/xtianxx/txharbor/internal/events"
	"github.com/xtianxx/txharbor/internal/execution"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// e2eJSONDo issues one authenticated JSON request and returns the status and
// body.
func e2eJSONDo(t *testing.T, method, url, token string, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, raw
}

// TestE2EWithdrawalRequestExecutionEvents is the T079 acceptance.
func TestE2EWithdrawalRequestExecutionEvents(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := e2eStartPostgres(t)
	rpcURL := e2eStartAnvil(t)
	kafka := e2eStartKafka(t)
	redisCtr := e2eStartRedis(t)
	pool := e2eOpenPool(t, dsn)
	env := e2eBaseEnv(dsn, rpcURL, e2eFreeAddr(t), kafka.Brokers())
	env["TXHARBOR_REDIS_ADDR"] = redisCtr.HostPort()

	const (
		sender       = "0x5151305151305151305151305151305151305151"
		recipient    = "0x3333333333333333333333333333333333333333"
		authorizeID  = "authz-e2e-1"
		amount       = "1000"
		idempotencyK = "idem-e2e-1"
	)
	// Seed the 007/011 prerequisites: caller, API key, execution permission,
	// asset allowlist, active grant, active nonce scope.
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label, can_create) VALUES ($1, 'e2e', TRUE)
		ON CONFLICT (caller_id) DO UPDATE SET can_create = TRUE`, e2eCaller); err != nil {
		t.Fatalf("seed caller: %v", err)
	}
	apiKey, _, err := withdrawal.IssueKey(ctx, pool, e2eCaller, "e2e")
	if err != nil {
		t.Fatalf("IssueKey: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO execution_caller_permission (caller_id, can_execute, updated_by)
		VALUES ($1, TRUE, 'e2e') ON CONFLICT (caller_id) DO UPDATE SET can_execute = TRUE`, e2eCaller); err != nil {
		t.Fatalf("seed execution permission: %v", err)
	}
	// The deposit scanner bootstraps deposit_config_history from the serve
	// configuration (assets/watches); the 007 allowlist reader consumes the
	// newest version, so no seed is inserted here (a foreign version would
	// refuse the scanner startup).
	if _, err := withdrawal.SupplyGrant(ctx, pool, withdrawal.OpInput{
		OperationID:     "op-e2e-supply",
		Action:          "supply",
		AuthorizationID: authorizeID,
		CallerID:        e2eCaller,
		ChainID:         31337,
		Asset:           e2eAsset,
		Recipient:       recipient,
		Amount:          amount,
	}, "e2e", "controlled e2e supply"); err != nil {
		t.Fatalf("SupplyGrant: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq)
		VALUES (31337, $1, 'active', 1) ON CONFLICT (chain_id, sender) DO UPDATE SET state = 'active'`, sender); err != nil {
		t.Fatalf("seed nonce registry: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES (31337, $1)
		ON CONFLICT (chain_id, sender) DO NOTHING`, sender); err != nil {
		t.Fatalf("seed nonce scope state: %v", err)
	}

	// The real serve process: the 007 create handler and the 011 admission
	// handler on one listener.
	var serveErr e2eBuffer
	serveStop, serveDone := e2eRunCommand(ctx, func(runCtx context.Context) int {
		return Serve(runCtx, Deps{
			Getenv:  e2eGetenv(env),
			Stdout:  io.Discard,
			Stderr:  &serveErr,
			Signals: make(chan os.Signal),
		})
	})
	baseURL := "http://" + env["TXHARBOR_HTTP_ADDR"]
	e2eWaitHTTPOrFail(t, baseURL+"/readyz", http.StatusOK, 90*time.Second, nil, &serveErr)
	// The deposit scanner boots the 007 allowlist version from configuration;
	// wait for it before the create call so the request never races startup.
	e2eWait(t, 60*time.Second, "deposit config history bootstrap", func() bool {
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deposit_config_history WHERE chain_id = 31337`).Scan(&n); err != nil {
			return false
		}
		return n > 0
	})

	// Real 007 request over HTTP: the receipt is persisted and its
	// withdrawal.request.received event lands in the same transaction.
	createBody, _ := json.Marshal(map[string]any{
		"idempotency_key":  idempotencyK,
		"chain_id":         31337,
		"asset":            e2eAsset,
		"recipient":        recipient,
		"amount":           amount,
		"authorization_id": authorizeID,
	})
	status, raw := e2eJSONDo(t, http.MethodPost, baseURL+"/withdrawals", apiKey, string(createBody))
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("007 create status=%d body=%s", status, raw)
	}
	var created struct {
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.RequestID == "" {
		t.Fatalf("007 create body=%s err=%v", raw, err)
	}
	if created.Status != "accepted" {
		t.Fatalf("007 create status field = %q, want accepted", created.Status)
	}

	// The event is durable and carries the Accepted semantics; it is not an
	// execution authorization.
	var (
		eventVersion int64
		eventPayload []byte
		eventSource  string
	)
	if err := pool.QueryRow(ctx, `
SELECT aggregate_version, payload, source_kind FROM outbox_events
WHERE event_type = 'withdrawal.request.received' AND aggregate_id = $1`, created.RequestID).
		Scan(&eventVersion, &eventPayload, &eventSource); err != nil {
		t.Fatalf("read withdrawal.request.received row: %v", err)
	}
	if eventVersion != 1 {
		t.Fatalf("request.received version = %d, want 1", eventVersion)
	}
	var payload map[string]any
	if err := json.Unmarshal(eventPayload, &payload); err != nil {
		t.Fatalf("decode request.received payload: %v", err)
	}
	if payload["state"] != "accepted" || payload["request_id"] != created.RequestID {
		t.Fatalf("request.received payload = %v, want the accepted fact", payload)
	}

	// Duplicate request: no second request row and no second event.
	status, raw = e2eJSONDo(t, http.MethodPost, baseURL+"/withdrawals", apiKey, string(createBody))
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("duplicate 007 create status=%d body=%s", status, raw)
	}
	var duplicate struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &duplicate); err != nil || duplicate.RequestID != created.RequestID {
		t.Fatalf("duplicate create request_id = %q (err %v), want %s", duplicate.RequestID, err, created.RequestID)
	}
	var requestRows, requestEvents int64
	if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM withdrawal_requests WHERE caller_id = $1 AND idempotency_key = $2),
       (SELECT count(*) FROM outbox_events WHERE event_type = 'withdrawal.request.received' AND aggregate_id = $3)`,
		e2eCaller, idempotencyK, created.RequestID).Scan(&requestRows, &requestEvents); err != nil {
		t.Fatalf("count request rows/events: %v", err)
	}
	if requestRows != 1 || requestEvents != 1 {
		t.Fatalf("duplicate request produced rows=%d events=%d, want (1, 1)", requestRows, requestEvents)
	}

	// Controlled fee scope for the intent (test-supplied authorization fact):
	// the scope row is the 011 admission's identity basis and must exist
	// before the admission call, exactly like the real authorization supply.
	const intentID = "intent-e2e-1"
	if _, err := pool.Exec(ctx, `INSERT INTO withdrawal_authorization_scopes
		(authorization_id, intent_id, request_id, sender, fee_max_total, fee_max_per_gas,
		 fee_max_priority, allows_fee_replacement, authorization_version, attested_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, TRUE, 1, 'e2e')`,
		authorizeID, intentID, created.RequestID, sender, int64(1e15), int64(2e9), int64(15e8)); err != nil {
		t.Fatalf("seed authorization scope: %v", err)
	}

	// Real 011 admission over HTTP.
	status, raw = e2eJSONDo(t, http.MethodPost, baseURL+"/withdrawals/"+created.RequestID+"/execution", apiKey, "")
	if status != http.StatusCreated {
		t.Fatalf("011 admit status=%d body=%s", status, raw)
	}
	var admitted struct {
		IntentID string `json:"intent_id"`
	}
	if err := json.Unmarshal(raw, &admitted); err != nil || admitted.IntentID == "" {
		t.Fatalf("011 admit body=%s err=%v", raw, err)
	}
	if admitted.IntentID != intentID {
		t.Fatalf("011 admit intent_id = %q, want %q", admitted.IntentID, intentID)
	}

	// Drive the real 011 state machine through the same public functions the
	// worker uses: a real claim, then the send-enabling and factual
	// transitions. Each committed transition emits its state_changed event.
	claimStore, err := execution.NewClaimStore(pool, 30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewClaimStore: %v", err)
	}
	claim, err := claimStore.Claim(ctx, admitted.IntentID, "e2e-owner")
	if err != nil || !claim.Acquired {
		t.Fatalf("Claim = %+v (err %v), want acquired", claim, err)
	}
	e2eTransition(t, pool, admitted.IntentID, execution.IntentAdmitted, execution.IntentClaimed, claim.Version)
	e2eTransition(t, pool, admitted.IntentID, execution.IntentClaimed, execution.IntentExecuting, claim.Version)
	e2eTransition(t, pool, admitted.IntentID, execution.IntentExecuting, execution.IntentCompleted, claim.Version)

	// The execution events exist with continuous versions; the request keeps
	// its Accepted semantics through the whole flow.
	var executionEvents, executionVersions int64
	if err := pool.QueryRow(ctx, `
SELECT count(*), max(aggregate_version) FROM outbox_events
WHERE event_type = 'withdrawal.execution.state_changed' AND aggregate_id = $1`, admitted.IntentID).
		Scan(&executionEvents, &executionVersions); err != nil {
		t.Fatalf("count execution events: %v", err)
	}
	if executionEvents != 3 || executionVersions != 3 {
		t.Fatalf("execution events = (count %d, max version %d), want (3, 3)", executionEvents, executionVersions)
	}
	var requestStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM withdrawal_requests WHERE request_id = $1`, created.RequestID).Scan(&requestStatus); err != nil {
		t.Fatalf("read request status: %v", err)
	}
	if requestStatus != "accepted" {
		t.Fatalf("request status = %s, want accepted (the events never change receipt semantics)", requestStatus)
	}

	// Snapshot the upstream authority counts: publishing and consuming the
	// events must not create an intent, nonce, signature or broadcast.
	authorityTables := []string{"withdrawal_requests", "payment_intents", "nonce_bindings",
		"tx_attempts", "tx_attempt_signings", "tx_send_attempts"}
	before := map[string]int64{}
	for _, table := range authorityTables {
		var n int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		before[table] = n
	}

	// Real publisher + real reference consumer over the real broker.
	pubStop, pubDone := e2eRunCommand(ctx, func(runCtx context.Context) int {
		return EventPublisher(runCtx, nil, Deps{Getenv: e2eGetenv(env), Stdout: io.Discard, Stderr: io.Discard})
	})
	e2eWait(t, 120*time.Second, "outbox drain", func() bool {
		var pending int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM outbox_events WHERE publish_state = 'pending'`).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	})
	e2eStopCommand(t, pubStop, pubDone, "event-publisher")

	consumerStop, consumerDone := e2eRunCommand(ctx, func(runCtx context.Context) int {
		return EventConsumer(runCtx, nil, Deps{Getenv: e2eGetenv(env), Stdout: io.Discard, Stderr: io.Discard})
	})
	e2eWait(t, 180*time.Second, "reference consumer effects", func() bool {
		var n int64
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM ref_consumer_ledger WHERE consumer_name = $1`, "txharbor.ref-consumer.v1").Scan(&n); err != nil {
			return false
		}
		return n == 4 // one request.received + three state_changed
	})
	e2eStopCommand(t, consumerStop, consumerDone, "event-consumer")
	e2eStopCommand(t, serveStop, serveDone, "serve")

	// Effective application exactly once per event, and no send-side action.
	var maxPerEvent int64
	if err := pool.QueryRow(ctx, `
SELECT coalesce(max(rows), 0) FROM (
	SELECT count(*) AS rows FROM ref_consumer_ledger WHERE consumer_name = $1 GROUP BY event_id
) counted`, "txharbor.ref-consumer.v1").Scan(&maxPerEvent); err != nil {
		t.Fatalf("max ledger rows per event: %v", err)
	}
	if maxPerEvent != 1 {
		t.Fatalf("max ledger rows per event = %d, want 1 (no duplicate effect)", maxPerEvent)
	}
	for _, table := range authorityTables {
		var n int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != before[table] {
			t.Fatalf("%s rows changed through publish/consume: %d -> %d (a processed event is not a send permission)",
				table, before[table], n)
		}
	}
	if !strings.Contains(events.ReferenceBoundaryStatement, "external real ledger") {
		t.Fatal("the FR-16 boundary statement is missing from the evidence")
	}
}

// e2eTransition applies one real 011 intent transition under its current
// state version and commits it (the same call the worker makes).
func e2eTransition(t *testing.T, pool *pgxpool.Pool, intentID, from, to string, leaseVersion int64) {
	t.Helper()
	ctx := context.Background()
	var stateVersion int64
	if err := pool.QueryRow(ctx,
		`SELECT state_version FROM payment_intents WHERE intent_id = $1`, intentID).Scan(&stateVersion); err != nil {
		t.Fatalf("read state_version for %s: %v", intentID, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transition: %v", err)
	}
	if err := execution.TransitionIntent(ctx, tx, intentID, from, stateVersion, to, leaseVersion); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("TransitionIntent(%s -> %s): %v", from, to, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transition %s -> %s: %v", from, to, err)
	}
}
