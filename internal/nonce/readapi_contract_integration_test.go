//go:build integration

// readapi_contract_integration_test.go owns spec task T034 for
// 008-nonce-manager (US5; FR-14/19; read-api.md §2/§3/§4; V9; SC-06/09): the
// provider-side acceptance of the read-only 008 -> 009 binding contract.
//
// The 009 client is out of scope. Every assertion is driven through the REAL
// HTTP endpoints mounted in internal/app/serve.go ("txharbor serve"), over a
// real PostgreSQL and a scripted JSON-RPC double, with a bearer credential —
// never through the carrier function directly.
//
// What it proves:
//   - each of the five outcomes (`bound`/`terminal`/`not_bound`/`mismatch`/
//     `unavailable`) matches contracts/read-api.md §2/§3 exactly: status code,
//     the exact §3.1/§3.2 field set (no extra/missing keys at any level), the
//     fixed `notice`, decimal-string nonce, and the `mismatch` durable-scope
//     echo;
//   - authentication: 401 without a bearer and 401 with a wrong bearer;
//   - read immutability: every 008 table plus every 006/007 domain table is
//     byte-identical across a batch of reads;
//   - a release/establish racing a held-open read is never straddled (one
//     snapshot: binding state and gate causes always agree);
//   - a 006 read failure yields recovery.state="unknown", never none/released.
//
// Every helper this file owns is `rc`-prefixed so it cannot collide with a
// sibling integration test. The container/migration pattern is reused from
// migration_integration_test.go (nonceStartPostgres / nonceMigrateOptions /
// nonceOpenSQL / nonceMustExec / nonceAddr).
package nonce_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/app"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/nonce"
)

const (
	rcChain     = int64(80231)
	rcChain2    = rcChain + 1
	rcToken     = "rc-read-token"
	rcWrong     = "rc-wrong-token"
	rcMissing   = "nb-00000000000000000000000000000000"
	rcAuthID    = "wa-rc-1"
	rcPolicyMax = 64
)

// rcAuthVer is the 64-hex authorization-version digest the seeds use.
var rcAuthVer = strings.Repeat("a", 64)

// One dedicated sender scope per scenario keeps every binding, hold and
// annotation independent.
var (
	rcSenderOpen   = nonceAddr("f1")
	rcSenderHeld   = nonceAddr("f2")
	rcSenderCons   = nonceAddr("f3")
	rcSenderRel    = nonceAddr("f4")
	rcSenderMis    = nonceAddr("f5")
	rcSenderRace   = nonceAddr("f6")
	rcSenderUnav   = nonceAddr("f7")
	rcSenderUnk    = nonceAddr("f8") // chain2, 006-unknown scenario
	rcMainSenders  = []string{rcSenderOpen, rcSenderHeld, rcSenderCons, rcSenderRel, rcSenderMis, rcSenderRace, rcSenderUnav}
	rcHoldHeldGap  = "rc-hold-gap"
	rcHoldHeldDiv  = "rc-hold-div"
	rcHoldRaceA    = "rc-hold-race-a"
	rcHoldRaceB    = "rc-hold-race-b"
	rcBindRaceOpA  = "rc-op-race-a"
	rcIntentRace   = "rc-intent-race"
	rcBindOpen     = "rc-b-open"
	rcBindHeld     = "rc-b-held"
	rcBindConsumed = "rc-b-consumed"
	rcBindReleased = "rc-b-released"
	rcBindMismatch = "rc-b-mismatch"
	rcBindRace     = "rc-b-race"
	rcBindUnavail  = "rc-b-unavail"
	rcBindUnknown  = "rc-b-unknown"
	rcRelOpID      = "rc-op-released"
)

// rcEightTables are the seven 008-owned tables a read MUST never touch.
var rcEightTables = []string{
	"nonce_wallet_registry", "nonce_scope_state", "nonce_bindings",
	"nonce_binding_events", "nonce_observations", "nonce_scope_holds",
	"nonce_ops_audit",
}

// rcUpstreamTables are every 006/007 domain table 008 reads but must never
// write. indexer_lease is excluded: it is the shared coordination row, not
// 006/007 domain data (002-owned).
var rcUpstreamTables = []string{
	// 006
	"reorg_policy_history", "reorg_recovery", "reorg_recovery_events",
	"deposit_observation_transitions", "indexer_pause", "log_pause", "deposit_pause",
	// 007
	"caller", "api_key", "withdrawal_requests", "withdrawal_authorizations",
	"withdrawal_request_audit", "withdrawal_grant_audit",
}

// rcSyncWriter is a concurrency-safe io.Writer: serve.go writes its stdout from
// the Serve goroutine while the test polls for the listening line.
type rcSyncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *rcSyncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *rcSyncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// rcJSONRPCServer is a real HTTP JSON-RPC endpoint. The observation reads
// ("latest" head) resolve to a self-consistent healthy view (latest ==
// pending == 0, head 0) so every reconcile pass is `consistent`; numeric block
// queries return null (not-found → the indexer waits, writes nothing).
func rcJSONRPCServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []any           `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var result any
		switch req.Method {
		case "eth_chainId":
			result = fmt.Sprintf("0x%x", rcChain)
		case "eth_getTransactionCount":
			result = "0x0"
		case "eth_getBlockByNumber":
			if len(req.Params) > 0 && req.Params[0] == "latest" {
				result = map[string]any{
					"number": "0x0",
					"hash":   "0x" + strings.Repeat("cd", 32),
				}
			} else {
				result = nil
			}
		case "eth_getLogs":
			result = []any{}
		default:
			result = nil
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// rcSeed inserts the 007 caller/authorization, the 006 policy row, the 008
// registry/scope rows, every binding, the holds, and the chain2 released-event
// marker the 006-unknown scenario needs.
func rcSeed(t *testing.T, sqlDB *sql.DB) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO caller (caller_id, label) VALUES (1, 'rc-it')`)
	nonceMustExec(t, sqlDB, `INSERT INTO withdrawal_authorizations
		(authorization_id, caller_id, chain_id, asset, recipient, amount, state)
		VALUES ($1, 1, $2, $3, $4, 1, 'active')`, rcAuthID, rcChain, nonceAddr("c1"), nonceAddr("d1"))
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_policy_history
		(chain_id, policy_seq, max_depth, prev_seq, operator, request_id)
		VALUES ($1, 1, $2, NULL, 'bootstrap', NULL)`, rcChain, rcPolicyMax)

	for _, sender := range append(append([]string{}, rcMainSenders...), rcSenderUnk) {
		chain := rcChain
		if sender == rcSenderUnk {
			chain = rcChain2
		}
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_wallet_registry
			(chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 3)`, chain, sender)
		nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_state (chain_id, sender) VALUES ($1, $2)`, chain, sender)
	}

	rcInsertBinding(t, sqlDB, rcChain, rcSenderOpen, rcBindOpen, "rc-intent-open", nonce.StateAllocated, "0")
	rcInsertBinding(t, sqlDB, rcChain, rcSenderHeld, rcBindHeld, "rc-intent-held", nonce.StateAllocated, "0")
	rcInsertBinding(t, sqlDB, rcChain, rcSenderCons, rcBindConsumed, "rc-intent-consumed", nonce.StateConsumed, "0")
	rcInsertBinding(t, sqlDB, rcChain, rcSenderRel, rcBindReleased, "rc-intent-released", nonce.StateReleased, "0")
	rcInsertBinding(t, sqlDB, rcChain, rcSenderMis, rcBindMismatch, "rc-intent-mismatch", nonce.StateAllocated, "0")
	rcInsertBinding(t, sqlDB, rcChain, rcSenderRace, rcBindRace, rcIntentRace, nonce.StateAllocated, "0")
	rcInsertBinding(t, sqlDB, rcChain, rcSenderUnav, rcBindUnavail, "rc-intent-unavail", nonce.StateAllocated, "0")
	rcInsertBinding(t, sqlDB, rcChain2, rcSenderUnk, rcBindUnknown, "rc-intent-unknown", nonce.StateAllocated, "0")

	rcInsertHold(t, sqlDB, rcChain, rcSenderHeld, rcHoldHeldGap, nonce.CauseUnexplainedGap, "2026-01-01T00:00:00Z")
	rcInsertHold(t, sqlDB, rcChain, rcSenderHeld, rcHoldHeldDiv, nonce.CauseChainViewDivergence, "2026-01-01T00:00:01Z")
	rcInsertHold(t, sqlDB, rcChain, rcSenderRace, rcHoldRaceA, nonce.CauseUnexplainedGap, "2026-01-01T00:00:02Z")

	// The chain2 released marker: a normal read of chain2 would report
	// `released`; a 006 read failure MUST instead report `unknown`.
	nonceMustExec(t, sqlDB, `INSERT INTO reorg_recovery_events
		(chain_id, recovery_id, recovery_seq, event_seq, event, detail)
		VALUES ($1, 'rc-rec-2', 1, 1, 'released', 'rc-seed')`, rcChain2)
}

// rcInsertBinding inserts one durable binding row directly, carrying whatever
// terminal fact its state requires (000008's terminal-consistency CHECK).
func rcInsertBinding(t *testing.T, sqlDB *sql.DB, chainID int64, sender, bindingID, intentID, state, nonceText string) {
	t.Helper()
	consumedAt, releasedAt, releaseOp := "NULL", "NULL", "NULL"
	switch state {
	case nonce.StateConsumed:
		consumedAt = "now()"
	case nonce.StateReleased:
		releasedAt = "now()"
		releaseOp = "'" + rcRelOpID + "'"
	}
	query := fmt.Sprintf(`INSERT INTO nonce_bindings
		(binding_id, intent_id, chain_id, sender, nonce, state, authorization_id,
		 authorization_version, registry_seq, allocation_observation_id,
		 consumed_at, released_at, release_operation_id)
		VALUES ($1, $2, $3, $4, $5::numeric, $6, $7, $8, 3, 'no-rc-prev', %s, %s, %s)`,
		consumedAt, releasedAt, releaseOp)
	nonceMustExec(t, sqlDB, query, bindingID, intentID, chainID, sender, nonceText, state, rcAuthID, rcAuthVer)
}

// rcInsertHold inserts one active hold with an explicit established_at so the
// gate causes order is deterministic (readActiveHoldsTx orders by
// established_at, hold_id).
func rcInsertHold(t *testing.T, sqlDB *sql.DB, chainID int64, sender, holdID, cause, establishedAt string) {
	t.Helper()
	nonceMustExec(t, sqlDB, `INSERT INTO nonce_scope_holds
		(hold_id, chain_id, sender, cause, status, established_at, evidence_observation_id, evidence_detail)
		VALUES ($1, $2, $3, $4, 'active', $5::timestamptz, 'rc-ev', 'rc-seed')`,
		holdID, chainID, sender, cause, establishedAt)
}

// rcSnapshot renders each table's full row set as one canonical string;
// equality of the returned maps is the byte-identity assertion.
func rcSnapshot(t *testing.T, sqlDB *sql.DB) map[string]string {
	t.Helper()
	tables := append(append([]string{}, rcEightTables...), rcUpstreamTables...)
	out := make(map[string]string, len(tables))
	for _, table := range tables {
		var blob string
		query := fmt.Sprintf(
			`SELECT COALESCE(string_agg(t::text, E'\n' ORDER BY t::text), '<empty>') FROM %s t`, table)
		if err := sqlDB.QueryRow(query).Scan(&blob); err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		out[table] = blob
	}
	return out
}

// rcWantUnchanged asserts every 008 + 006/007 table is byte-identical.
func rcWantUnchanged(t *testing.T, before, after map[string]string) {
	t.Helper()
	for table, blob := range before {
		if after[table] != blob {
			t.Fatalf("read changed table %s:\nbefore=%q\nafter =%q", table, blob, after[table])
		}
	}
}

// rcHTTP is the HTTP client bound to the serve listener.
type rcHTTP struct {
	client *http.Client
	base   string
}

// rcGet performs one GET (no t.Fatal; safe to call from a goroutine). An empty
// token omits the Authorization header.
func (h rcHTTP) rcGet(path, token string) (int, []byte, error) {
	req, err := http.NewRequest(http.MethodGet, h.base+path, nil)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, raw, nil
}

// rcMustGet performs one GET and decodes the JSON body.
func rcMustGet(t *testing.T, h rcHTTP, path, token string) (int, map[string]any) {
	t.Helper()
	status, raw, err := h.rcGet(path, token)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	var m map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("decode %s: %v (body %s)", path, err, raw)
		}
	}
	return status, m
}

// rcWantKeys asserts obj's key set is exactly want (sorted comparison).
func rcWantKeys(t *testing.T, where string, obj map[string]any, want ...string) {
	t.Helper()
	got := make([]string, 0, len(obj))
	for k := range obj {
		got = append(got, k)
	}
	sort.Strings(got)
	exp := append([]string{}, want...)
	sort.Strings(exp)
	if strings.Join(got, ",") != strings.Join(exp, ",") {
		t.Fatalf("%s keys = %v, want exactly %v", where, got, exp)
	}
}

func rcObj(t *testing.T, where string, parent map[string]any, key string) map[string]any {
	t.Helper()
	v, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s.%s = %#v, want an object", where, key, parent[key])
	}
	return v
}

func rcStr(t *testing.T, where string, m map[string]any, key string) string {
	t.Helper()
	v, ok := m[key].(string)
	if !ok {
		t.Fatalf("%s.%s = %#v, want a string", where, key, m[key])
	}
	return v
}

func rcNum(t *testing.T, where string, m map[string]any, key string) float64 {
	t.Helper()
	v, ok := m[key].(float64)
	if !ok {
		t.Fatalf("%s.%s = %#v, want a JSON number", where, key, m[key])
	}
	return v
}

// rcWantBoundShape pins the exact §3.1/§3.2 field set and every nested shape.
func rcWantBoundShape(t *testing.T, where string, m map[string]any, terminal, released bool) {
	t.Helper()
	rcWantKeys(t, where, m, "outcome", "binding", "annotations", "notice")
	b := rcObj(t, where, m, "binding")
	keys := []string{"authorization", "binding_id", "chain_id", "created_at", "intent_id", "nonce", "registry_seq", "sender", "state"}
	if terminal {
		keys = append(keys, "terminal_at")
	}
	if released {
		keys = append(keys, "release_operation_id")
	}
	rcWantKeys(t, where+".binding", b, keys...)
	auth := rcObj(t, where+".binding", b, "authorization")
	rcWantKeys(t, where+".binding.authorization", auth, "id", "version")
	ann := rcObj(t, where, m, "annotations")
	rcWantKeys(t, where+".annotations", ann, "gate", "recovery", "registry_state")
	gate := rcObj(t, where+".annotations", ann, "gate")
	rcWantKeys(t, where+".annotations.gate", gate, "state", "causes")
	rcWantKeys(t, where+".annotations.recovery", rcObj(t, where+".annotations", ann, "recovery"), "state")
	if got := gate["causes"].([]any); len(got) == 0 {
		if s := rcStr(t, where+".annotations.gate", gate, "state"); s != "open" {
			t.Fatalf("%s empty causes with gate.state=%q, want open", where, s)
		}
	} else if s := rcStr(t, where+".annotations.gate", gate, "state"); s != "held" {
		t.Fatalf("%s non-empty causes with gate.state=%q, want held", where, s)
	}
	for i, c := range gate["causes"].([]any) {
		rcWantKeys(t, fmt.Sprintf("%s.causes[%d]", where, i), c.(map[string]any), "cause", "established_at", "hold_id")
	}
	if got := rcStr(t, where, m, "notice"); got != nonce.ReadNoticeFacts {
		t.Fatalf("%s.notice = %q, want the fixed facts notice", where, got)
	}
}

// rcWantErrorShape pins the exact §3.3 body shape (code + request trace id, no
// internal detail).
func rcWantErrorShape(t *testing.T, where string, m map[string]any, outcome string, extra ...string) {
	t.Helper()
	rcWantKeys(t, where, m, append([]string{"outcome", "error", "notice"}, extra...)...)
	e := rcObj(t, where, m, "error")
	rcWantKeys(t, where+".error", e, "code", "request_trace_id")
	if got := rcStr(t, where+".error", e, "code"); got != outcome {
		t.Fatalf("%s.error.code = %q, want %q", where, got, outcome)
	}
}

// rcWantDecimalNonce asserts nonce is a decimal string (never a float).
func rcWantDecimalNonce(t *testing.T, where string, m map[string]any) {
	t.Helper()
	s := rcStr(t, where, m, "nonce")
	if _, err := strconv.ParseUint(s, 10, 64); err != nil {
		t.Fatalf("%s.nonce = %q, want a decimal string in uint64 range", where, s)
	}
}

// rcLockScopeForUpdate holds the scope row until the returned tx commits; a
// concurrent read blocks at its scope FOR SHARE.
func rcLockScopeForUpdate(t *testing.T, sqlDB *sql.DB, chainID int64, sender string) *sql.Tx {
	t.Helper()
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin control tx: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.Exec(`SET LOCAL lock_timeout = '10s'`); err != nil {
		t.Fatalf("set lock_timeout: %v", err)
	}
	var cid int64
	if err := tx.QueryRow(`SELECT chain_id FROM nonce_scope_state
		WHERE chain_id = $1 AND sender = $2 FOR UPDATE`, chainID, sender).Scan(&cid); err != nil {
		t.Fatalf("lock scope row FOR UPDATE: %v", err)
	}
	return tx
}

// rcWaitBlockedRead waits until want backends are blocked acquiring the scope
// FOR SHARE (the read's first statement under the racing control tx).
func rcWaitBlockedRead(t *testing.T, sqlDB *sql.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := sqlDB.QueryRow(`SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query ILIKE '%nonce_scope_state%' AND query ILIKE '%FOR SHARE%'`).Scan(&n); err != nil {
			t.Fatalf("poll blocked read: %v", err)
		}
		if n >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the racing read never blocked on the scope FOR SHARE")
}

// rcWantNoStraddle asserts the racing read's binding state and gate causes came
// from ONE snapshot: `allocated` with the pre-release hold, or `in_flight` with
// the fresh hold — never a composite.
func rcWantNoStraddle(t *testing.T, where string, m map[string]any) {
	t.Helper()
	b := rcObj(t, where, m, "binding")
	ann := rcObj(t, where, m, "annotations")
	gate := rcObj(t, where+".annotations", ann, "gate")
	state := rcStr(t, where+".binding", b, "state")
	causes, _ := gate["causes"].([]any)
	ids := make([]string, 0, len(causes))
	for _, c := range causes {
		ids = append(ids, rcStr(t, where+".causes", c.(map[string]any), "hold_id"))
	}
	sort.Strings(ids)
	switch state {
	case nonce.StateAllocated:
		if strings.Join(ids, ",") != rcHoldRaceA {
			t.Fatalf("%s: allocated snapshot must carry ONLY %s, got causes %v (straddled)", where, rcHoldRaceA, ids)
		}
	case nonce.StateInFlight:
		if strings.Join(ids, ",") != rcHoldRaceB {
			t.Fatalf("%s: in_flight snapshot must carry ONLY %s, got causes %v (straddled)", where, rcHoldRaceB, ids)
		}
	default:
		t.Fatalf("%s.state = %q, want allocated or in_flight", where, state)
	}
}

// rcAppDSN returns the container DSN rewritten for the non-owner read role so
// REVOKE SELECT deterministically fails a table read (the owner's ACL cannot).
func rcAppDSN(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.User = url.UserPassword("rc_app", "rc-pass")
	return u.String()
}

func TestNonceReadAPIContractIntegration(t *testing.T) {
	dsn := nonceStartPostgres(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var out bytes.Buffer
	if err := db.MigrateUp(ctx, nonceMigrateOptions(dsn), &out); err != nil {
		t.Fatalf("MigrateUp() error = %v (output %q)", err, out.String())
	}
	sqlDB := nonceOpenSQL(t, dsn)
	rcSeed(t, sqlDB)

	// A non-owner role: only an ACL change can make an 008/006 table read fail
	// deterministically (the owner's privileges cannot be revoked).
	nonceMustExec(t, sqlDB, `CREATE ROLE rc_app LOGIN PASSWORD 'rc-pass'`)
	nonceMustExec(t, sqlDB, `GRANT CONNECT ON DATABASE txharbor TO rc_app`)
	nonceMustExec(t, sqlDB, `GRANT USAGE ON SCHEMA public TO rc_app`)
	nonceMustExec(t, sqlDB, `GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO rc_app`)
	nonceMustExec(t, sqlDB, `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO rc_app`)
	appDSN := rcAppDSN(t, dsn)

	rpcSrv := rcJSONRPCServer(t)

	stdout, stderr := &rcSyncWriter{}, &rcSyncWriter{}
	env := map[string]string{
		config.EnvPGDSN:                 appDSN,
		config.EnvRPCURL:                rpcSrv.URL,
		config.EnvChainID:               strconv.FormatInt(rcChain, 10),
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          nonceAddr("bb"),
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      nonceAddr("bb"),
		config.EnvDepositWatchAddresses: nonceAddr("bb"),
		config.EnvConfirmationDepth:     "12",
		config.EnvReorgMaxDepth:         "100",
		config.EnvHTTPAddr:              "127.0.0.1:0",
		config.EnvNonceReadToken:        rcToken,
		config.EnvIndexPollInterval:     "1h",
	}
	done := make(chan int, 1)
	go func() {
		done <- app.Serve(ctx, app.Deps{
			Getenv:  func(k string) (string, bool) { v, ok := env[k]; return v, ok },
			Stdout:  stdout,
			Stderr:  stderr,
			Signals: make(chan os.Signal),
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("serve exit code = %d, want 0 (stderr %q)", code, stderr.String())
			}
		case <-time.After(45 * time.Second):
			t.Errorf("serve did not shut down cleanly (stderr %q)", stderr.String())
		}
	})

	base := rcWaitListening(t, stdout)
	h := rcHTTP{client: &http.Client{Timeout: 30 * time.Second}, base: base}
	rcWaitReconcile(t, sqlDB, len(rcMainSenders))

	// --- read immutability: every read below leaves every table unchanged ---
	before := rcSnapshot(t, sqlDB)

	// --- §3.1 bound, gate open, exact field set -----------------------------
	t.Run("bound_gate_open", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindOpen, rcToken)
		if status != http.StatusOK {
			t.Fatalf("status = %d, body %v", status, m)
		}
		if got := rcStr(t, "bound", m, "outcome"); got != string(nonce.ReadBound) {
			t.Fatalf("outcome = %q, want bound", got)
		}
		rcWantBoundShape(t, "bound", m, false, false)
		b := rcObj(t, "bound", m, "binding")
		if got := rcStr(t, "bound.binding", b, "binding_id"); got != rcBindOpen {
			t.Fatalf("binding_id = %q", got)
		}
		if got := rcStr(t, "bound.binding", b, "state"); got != nonce.StateAllocated {
			t.Fatalf("state = %q, want allocated", got)
		}
		if got := rcStr(t, "bound.binding", b, "sender"); got != rcSenderOpen {
			t.Fatalf("sender = %q, want %q", got, rcSenderOpen)
		}
		rcWantDecimalNonce(t, "bound.binding", b)
		if got := rcNum(t, "bound.binding", b, "chain_id"); int64(got) != rcChain {
			t.Fatalf("chain_id = %v, want %d", got, rcChain)
		}
		if got := rcNum(t, "bound.binding", b, "registry_seq"); int64(got) != 3 {
			t.Fatalf("registry_seq = %v, want 3", got)
		}
		auth := rcObj(t, "bound.binding", b, "authorization")
		if got := rcStr(t, "bound.binding.authorization", auth, "id"); got != rcAuthID {
			t.Fatalf("authorization.id = %q, want %q", got, rcAuthID)
		}
		if got := rcStr(t, "bound.binding.authorization", auth, "version"); got != rcAuthVer {
			t.Fatalf("authorization.version mismatch")
		}
		if _, err := time.Parse(time.RFC3339, rcStr(t, "bound.binding", b, "created_at")); err != nil {
			t.Fatalf("created_at is not RFC3339 UTC: %v", err)
		}
		ann := rcObj(t, "bound", m, "annotations")
		gate := rcObj(t, "bound.annotations", ann, "gate")
		if got := rcStr(t, "bound.annotations.gate", gate, "state"); got != nonce.ReadGateOpen {
			t.Fatalf("gate.state = %q, want open", got)
		}
		if causes := gate["causes"].([]any); len(causes) != 0 {
			t.Fatalf("gate.causes = %v, want []", causes)
		}
		if got := rcStr(t, "bound.annotations", ann, "registry_state"); got != nonce.RegistryActive {
			t.Fatalf("registry_state = %q, want active", got)
		}
		if got := rcStr(t, "bound.annotations.recovery", rcObj(t, "bound.annotations", ann, "recovery"), "state"); got != nonce.RecoveryNone {
			t.Fatalf("recovery.state = %q, want none", got)
		}
	})

	// --- §3.1 bound, gate held, every active cause --------------------------
	t.Run("bound_gate_held_multi_cause", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindHeld, rcToken)
		if status != http.StatusOK {
			t.Fatalf("status = %d, body %v", status, m)
		}
		rcWantBoundShape(t, "held", m, false, false)
		gate := rcObj(t, "held.annotations", rcObj(t, "held", m, "annotations"), "gate")
		if got := rcStr(t, "held.annotations.gate", gate, "state"); got != nonce.ReadGateHeld {
			t.Fatalf("gate.state = %q, want held", got)
		}
		causes := gate["causes"].([]any)
		if len(causes) != 2 {
			t.Fatalf("gate.causes = %v, want both active holds", causes)
		}
		got := []string{
			rcStr(t, "causes[0]", causes[0].(map[string]any), "hold_id"),
			rcStr(t, "causes[1]", causes[1].(map[string]any), "hold_id"),
		}
		if got[0] != rcHoldHeldGap || got[1] != rcHoldHeldDiv {
			t.Fatalf("causes order = %v, want [%s %s] (established_at order)", got, rcHoldHeldGap, rcHoldHeldDiv)
		}
		for i, c := range causes {
			obj := c.(map[string]any)
			if _, err := time.Parse(time.RFC3339, rcStr(t, fmt.Sprintf("causes[%d]", i), obj, "established_at")); err != nil {
				t.Fatalf("causes[%d].established_at not RFC3339: %v", i, err)
			}
		}
	})

	// --- §3.2 terminal consumed / released ----------------------------------
	t.Run("terminal_consumed", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindConsumed, rcToken)
		if status != http.StatusOK {
			t.Fatalf("status = %d", status)
		}
		if got := rcStr(t, "terminal", m, "outcome"); got != string(nonce.ReadTerminal) {
			t.Fatalf("outcome = %q, want terminal", got)
		}
		rcWantBoundShape(t, "terminal", m, true, false)
		b := rcObj(t, "terminal", m, "binding")
		if got := rcStr(t, "terminal.binding", b, "state"); got != nonce.StateConsumed {
			t.Fatalf("state = %q, want consumed", got)
		}
		if s := rcStr(t, "terminal.binding", b, "terminal_at"); s == "" {
			t.Fatal("terminal_at is empty")
		}
	})

	t.Run("terminal_released", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindReleased, rcToken)
		if status != http.StatusOK {
			t.Fatalf("status = %d", status)
		}
		rcWantBoundShape(t, "terminal-rel", m, true, true)
		b := rcObj(t, "terminal-rel", m, "binding")
		if got := rcStr(t, "terminal-rel.binding", b, "state"); got != nonce.StateReleased {
			t.Fatalf("state = %q, want released", got)
		}
		if got := rcStr(t, "terminal-rel.binding", b, "release_operation_id"); got != rcRelOpID {
			t.Fatalf("release_operation_id = %q, want %q", got, rcRelOpID)
		}
	})

	// --- §2/§3.3 not_bound (404) and mismatch (409, durable echo) -----------
	t.Run("not_bound_404", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcMissing, rcToken)
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
		rcWantErrorShape(t, "not_bound", m, string(nonce.ReadNotBound))
		if got := rcStr(t, "not_bound", m, "notice"); got != nonce.ReadNoticeNotBound {
			t.Fatalf("notice = %q", got)
		}
	})

	t.Run("mismatch_409_durable_echo", func(t *testing.T) {
		path := fmt.Sprintf("/nonce/bindings/by-intent/%s?chain_id=%d&sender=%s",
			"rc-intent-mismatch", rcChain+5, rcSenderHeld)
		status, m := rcMustGet(t, h, path, rcToken)
		if status != http.StatusConflict {
			t.Fatalf("status = %d, want 409 (body %v)", status, m)
		}
		rcWantErrorShape(t, "mismatch", m, string(nonce.ReadMismatch), "binding_id", "chain_id", "sender")
		if got := rcStr(t, "mismatch", m, "binding_id"); got != rcBindMismatch {
			t.Fatalf("durable binding_id = %q, want %q", got, rcBindMismatch)
		}
		if got := rcNum(t, "mismatch", m, "chain_id"); int64(got) != rcChain {
			t.Fatalf("durable chain_id = %v, want %d", got, rcChain)
		}
		if got := rcStr(t, "mismatch", m, "sender"); got != rcSenderMis {
			t.Fatalf("durable sender = %q, want %q", got, rcSenderMis)
		}
		if got := rcStr(t, "mismatch", m, "notice"); got != nonce.ReadNoticeMismatch {
			t.Fatalf("notice = %q", got)
		}
	})

	// --- 401 without / with a wrong bearer ----------------------------------
	t.Run("auth_401", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindOpen, "")
		if status != http.StatusUnauthorized {
			t.Fatalf("missing bearer status = %d, want 401", status)
		}
		rcWantErrorShape(t, "unauth", m, string(nonce.ReadUnauthenticated))
		if got := rcStr(t, "unauth", m, "notice"); got != nonce.ReadNoticeUnauthenticated {
			t.Fatalf("notice = %q", got)
		}
		status, _ = rcMustGet(t, h, "/nonce/bindings/"+rcBindOpen, rcWrong)
		if status != http.StatusUnauthorized {
			t.Fatalf("wrong bearer status = %d, want 401", status)
		}
	})

	rcWantUnchanged(t, before, rcSnapshot(t, sqlDB))

	// --- single snapshot: a release+establish racing a held-open read --------
	t.Run("held_open_read_not_straddled", func(t *testing.T) {
		holder := rcLockScopeForUpdate(t, sqlDB, rcChain, rcSenderRace)
		type rcResult struct {
			status int
			body   []byte
			err    error
		}
		ch := make(chan rcResult, 1)
		path := fmt.Sprintf("/nonce/bindings/by-intent/%s?chain_id=%d&sender=%s", rcIntentRace, rcChain, rcSenderRace)
		go func() {
			st, raw, err := h.rcGet(path, rcToken)
			ch <- rcResult{st, raw, err}
		}()

		rcWaitBlockedRead(t, sqlDB, 1)

		// Release the read's pre-state hold and establish a new one, then move
		// the binding state — all in ONE commit.
		if _, err := holder.Exec(`UPDATE nonce_scope_holds SET status='released', released_at=now(),
			released_by='rc', release_operation_id=$2, release_evidence='rc', release_observation_id='rc-ev'
			WHERE hold_id=$1`, rcHoldRaceA, rcBindRaceOpA); err != nil {
			t.Fatalf("release racing hold: %v", err)
		}
		if _, err := holder.Exec(`INSERT INTO nonce_scope_holds
			(hold_id, chain_id, sender, cause, status, established_at, evidence_observation_id, evidence_detail)
			VALUES ($1, $2, $3, $4, 'active', now(), 'rc-ev', 'rc-race')`,
			rcHoldRaceB, rcChain, rcSenderRace, nonce.CauseUnexplainedGap); err != nil {
			t.Fatalf("establish racing hold: %v", err)
		}
		if _, err := holder.Exec(`UPDATE nonce_bindings SET state='in_flight', updated_at=now()
			WHERE binding_id=$1`, rcBindRace); err != nil {
			t.Fatalf("racing binding transition: %v", err)
		}
		if err := holder.Commit(); err != nil {
			t.Fatalf("commit racing writer: %v", err)
		}

		var res rcResult
		select {
		case res = <-ch:
		case <-time.After(30 * time.Second):
			t.Fatal("racing read did not complete after the writer committed")
		}
		if res.err != nil {
			t.Fatalf("racing read: %v", res.err)
		}
		if res.status != http.StatusOK {
			t.Fatalf("racing read status = %d (body %s)", res.status, res.body)
		}
		var m map[string]any
		if err := json.Unmarshal(res.body, &m); err != nil {
			t.Fatalf("decode racing body: %v", err)
		}
		rcWantBoundShape(t, "racing", m, false, false)
		rcWantNoStraddle(t, "racing", m)

		// The writer's post-state really landed: a fresh read sees it whole.
		status, after := rcMustGet(t, h, path, rcToken)
		if status != http.StatusOK {
			t.Fatalf("post-race read status = %d", status)
		}
		b := rcObj(t, "post-race", after, "binding")
		if got := rcStr(t, "post-race.binding", b, "state"); got != nonce.StateInFlight {
			t.Fatalf("post-race state = %q, want in_flight", got)
		}
		gate := rcObj(t, "post-race.annotations", rcObj(t, "post-race", after, "annotations"), "gate")
		causes := gate["causes"].([]any)
		if len(causes) != 1 || rcStr(t, "post-race.causes", causes[0].(map[string]any), "hold_id") != rcHoldRaceB {
			t.Fatalf("post-race causes = %v, want only %s", causes, rcHoldRaceB)
		}
	})

	// --- 006 read failure => recovery.state="unknown", never none/released ---
	t.Run("006_failure_is_unknown", func(t *testing.T) {
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindUnknown, rcToken)
		if status != http.StatusOK {
			t.Fatalf("status = %d", status)
		}
		if got := rcStr(t, "unknown.annotations.recovery", rcObj(t, "unknown.annotations", rcObj(t, "unknown", m, "annotations"), "recovery"), "state"); got != nonce.RecoveryReleased {
			t.Fatalf("pre-revoke recovery.state = %q, want released (the chain2 marker)", got)
		}

		nonceMustExec(t, sqlDB, `REVOKE SELECT ON reorg_recovery FROM rc_app`)
		status, m = rcMustGet(t, h, "/nonce/bindings/"+rcBindUnknown, rcToken)
		if status != http.StatusOK {
			t.Fatalf("006-failure status = %d, want a 200 with a degraded annotation", status)
		}
		got := rcStr(t, "unknown.annotations.recovery", rcObj(t, "unknown.annotations", rcObj(t, "unknown", m, "annotations"), "recovery"), "state")
		if got != nonce.RecoveryUnknown {
			t.Fatalf("recovery.state = %q, want unknown", got)
		}
		if got == nonce.RecoveryNone || got == nonce.RecoveryReleased {
			t.Fatalf("006 failure was mapped to %q; unknown is the only safe value", got)
		}
		nonceMustExec(t, sqlDB, `GRANT SELECT ON reorg_recovery TO rc_app`)
	})

	// --- 008 read failure => 503 unavailable, retryable, never absence ------
	t.Run("008_failure_is_unavailable", func(t *testing.T) {
		nonceMustExec(t, sqlDB, `REVOKE SELECT ON nonce_wallet_registry FROM rc_app`)
		status, m := rcMustGet(t, h, "/nonce/bindings/"+rcBindUnavail, rcToken)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", status)
		}
		rcWantErrorShape(t, "unavailable", m, string(nonce.ReadUnavailable))
		if got := rcStr(t, "unavailable", m, "notice"); got != nonce.ReadNoticeUnavailable {
			t.Fatalf("notice = %q, want the retry instruction", got)
		}
		nonceMustExec(t, sqlDB, `GRANT SELECT ON nonce_wallet_registry TO rc_app`)

		status, m = rcMustGet(t, h, "/nonce/bindings/"+rcBindUnavail, rcToken)
		if status != http.StatusOK {
			t.Fatalf("restored read status = %d, want 200", status)
		}
		if got := rcStr(t, "restored", m, "outcome"); got != string(nonce.ReadBound) {
			t.Fatalf("restored outcome = %q, want bound", got)
		}
	})
}

// rcWaitListening polls the serve stdout for the bound listener address.
func rcWaitListening(t *testing.T, stdout *rcSyncWriter) string {
	t.Helper()
	const marker = "listening on "
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if s := stdout.String(); strings.Contains(s, marker) {
			rest := s[strings.Index(s, marker)+len(marker):]
			addr := strings.TrimSpace(strings.SplitN(rest, "\n", 2)[0])
			return "http://" + addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("serve never reported a listening address (stdout %q)", stdout.String())
	return ""
}

// rcWaitReconcile waits for the initial reconcile pass (one observation per
// known scope) so the read-immutability window opens on a quiescent database.
func rcWaitReconcile(t *testing.T, sqlDB *sql.DB, scopes int) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := sqlDB.QueryRow(`SELECT count(*) FROM nonce_observations WHERE chain_id = $1`, rcChain).Scan(&n); err != nil {
			t.Fatalf("count observations: %v", err)
		}
		if n >= scopes {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("reconcile pass never completed: fewer than %d observations for chain %d", scopes, rcChain)
}
