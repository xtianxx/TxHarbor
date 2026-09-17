//go:build integration

// open3_dryrun_integration_test.go owns spec task T031
// (012-007-authorization-carrier): the OPEN-3 re-issuance dry-run, executed —
// not asserted from code reading — through the real T013 CLI carrier
// (`withdrawal-authz supply --reissue-from-authorization-id`) against a scratch
// PostgreSQL 18 container.
//
// The procedure mirrors quickstart.md "Re-issuance dry-run procedure (OPEN-3)":
//  1. pick a scopeless stock grant (queryable, never executable);
//  2. an authorized issuer re-verifies the business facts off-system (the
//     carrier re-resolves the presented API key through Authenticate ->
//     PermitIssue, and the supply tx repeats the api_key/caller `FOR SHARE`
//     re-read — T015);
//  3. explicit re-issuance via the extended supply: NEW grant id + scope in
//     one tx, audit detail linking the old grant/request ids (T030);
//  4. assert: trace complete, old rows byte/identity-identical, and zero new
//     intent/nonce rows (the seven 008 nonce_* tables, incl. the
//     intent_id-bearing nonce_bindings, are untouched).
//
// The re-issue reuses an already-recorded business intent so the distinct
// intent set is provably UNCHANGED ("zero new intent"): a stock grant carries
// no scope, so the dry-run's business intent is re-verified against the one
// recorded by the intent-holder grant below. There is no independent intent
// table in this tree (T030/H5); the scope table is the only intent record.
package app

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// open3NonceTables are the seven 008 tables a re-issue must not touch at all.
var open3NonceTables = []string{
	"nonce_wallet_registry",
	"nonce_scope_state",
	"nonce_bindings",
	"nonce_binding_events",
	"nonce_observations",
	"nonce_scope_holds",
	"nonce_ops_audit",
}

// open3Snapshot reads the FULL content of every named table as ordered
// row-JSON — the T030 technique (reissueSnapshot) — so the "untouched" compare
// is a content compare: an in-place UPDATE or an equal-size replacement is
// caught exactly like an insert or delete, not merely a count.
func open3Snapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tables []string) map[string][]string {
	t.Helper()
	snap := make(map[string][]string, len(tables))
	for _, table := range tables {
		rows, err := pool.Query(ctx, `SELECT row_to_json(t)::text FROM `+table+` t ORDER BY 1`)
		if err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		var out []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatalf("scan %s row: %v", table, err)
			}
			out = append(out, line)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("iterate %s: %v", table, err)
		}
		rows.Close()
		snap[table] = out
	}
	return snap
}

// open3Diff returns the multiset difference after\before and before\after.
func open3Diff(before, after []string) (added, removed []string) {
	counts := make(map[string]int, len(before))
	for _, r := range before {
		counts[r]++
	}
	for _, r := range after {
		if counts[r] > 0 {
			counts[r]--
			continue
		}
		added = append(added, r)
	}
	for r, n := range counts {
		for i := 0; i < n; i++ {
			removed = append(removed, r)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return added, removed
}

// open3SeedNonceFixture writes one representative row into each 008 nonce_*
// table (the T030 fixture, reissueSeedNonceFixture) so the "untouched" snapshot
// is a real content compare, never a vacuous empty set.
func open3SeedNonceFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sender string) {
	t.Helper()
	const chainID = int64(31337)
	stmts := []struct {
		name string
		sql  string
		args []any
	}{
		{"nonce_wallet_registry", `INSERT INTO nonce_wallet_registry (chain_id, sender, state, registry_seq) VALUES ($1, $2, 'active', 1)`, []any{chainID, sender}},
		{"nonce_scope_state", `INSERT INTO nonce_scope_state (chain_id, sender, reconciled_floor, last_latest, last_pending, last_observation_id) VALUES ($1, $2, 0, 0, 0, 'obs-init')`, []any{chainID, sender}},
		{"nonce_bindings", `INSERT INTO nonce_bindings (binding_id, intent_id, chain_id, sender, nonce, state, authorization_id, authorization_version, registry_seq, allocation_observation_id) VALUES ('bind-1', 'intent-nonce-1', $1, $2, 5, 'allocated', 'auth-nonce-1', repeat('a', 64), 1, 'obs-init')`, []any{chainID, sender}},
		{"nonce_binding_events", `INSERT INTO nonce_binding_events (binding_id, from_state, to_state, observation_id, operation_id, detail) VALUES ('bind-1', NULL, 'allocated', 'obs-init', 'op-init', 'seed')`, nil},
		{"nonce_observations", `INSERT INTO nonce_observations (observation_id, chain_id, sender, kind, classification, latest_count, pending_count, head_number, head_hash, error_class, rpc_ref) VALUES ('obs-init', $1, $2, 'allocation', 'consistent', 1, 0, 1, '0x' || repeat('b', 64), '', 'rpc-init')`, []any{chainID, sender}},
		{"nonce_scope_holds", `INSERT INTO nonce_scope_holds (hold_id, chain_id, sender, cause, status, evidence_observation_id) VALUES ('hold-1', $1, $2, 'unexplained_gap', 'active', 'obs-init')`, []any{chainID, sender}},
		{"nonce_ops_audit", `INSERT INTO nonce_ops_audit (operation_id, action, chain_id, sender, subject_id, outcome, operator, reason) VALUES ('op-nonce-seed', 'binding_release', $1, $2, 'bind-1', 'applied', 'op-seed', 'seed')`, []any{chainID, sender}},
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s.sql, s.args...); err != nil {
			t.Fatalf("seed %s: %v", s.name, err)
		}
	}
}

// open3DistinctIntents reads the distinct intent_id set the scope table carries.
func open3DistinctIntents(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT DISTINCT intent_id FROM withdrawal_authorization_scopes ORDER BY intent_id`)
	if err != nil {
		t.Fatalf("select distinct intent_id: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan distinct intent_id: %v", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate distinct intent_id: %v", err)
	}
	return out
}

// open3RowIdentity reads (ctid, xmin) for one row: a content-free proof the row
// was neither UPDATE'd nor delete+reinserted ("never rewrite applied rows").
func open3RowIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table, idColumn, id string) string {
	t.Helper()
	var ident string
	if err := pool.QueryRow(ctx,
		`SELECT ctid::text || '/' || xmin::text FROM `+table+` WHERE `+idColumn+` = $1`, id).Scan(&ident); err != nil {
		t.Fatalf("read %s identity for %q: %v", table, id, err)
	}
	return ident
}

// open3AuditRow reads (action, detail) for the recorded attempt with op id.
func open3AuditRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) (string, string) {
	t.Helper()
	var action, detail string
	if err := pool.QueryRow(ctx,
		`SELECT action, detail FROM withdrawal_grant_audit WHERE operation_id = $1`, operationID).
		Scan(&action, &detail); err != nil {
		t.Fatalf("read audit row for operation %q: %v", operationID, err)
	}
	return action, detail
}

// open3ScopeRead reads the single scope row for a grant, failing when it is
// absent (a stock grant has none).
func open3ScopeRead(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) (intentID, sender, attestedBy string, version int64) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
SELECT intent_id, sender, attested_by, authorization_version
FROM withdrawal_authorization_scopes
WHERE authorization_id = $1`, authorizationID).
		Scan(&intentID, &sender, &attestedBy, &version); err != nil {
		t.Fatalf("scope row for grant %q missing/unreadable: %v", authorizationID, err)
	}
	return intentID, sender, attestedBy, version
}

// open3SeedRequest writes the old signing-request row a re-issue links to.
func open3SeedRequest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string, callerID int64, authID, asset, recipient, amount string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO withdrawal_requests
    (request_id, caller_id, idempotency_key, authorization_id, chain_id, asset, recipient, amount)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::numeric)`,
		requestID, callerID, "idem-"+requestID, authID, 31337, asset, recipient, amount); err != nil {
		t.Fatalf("seed old request: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO withdrawal_request_audit (request_id, caller_id, action) VALUES ($1, $2, 'created')`,
		requestID, callerID); err != nil {
		t.Fatalf("seed old request audit: %v", err)
	}
}

// TestWithdrawalAuthzOpen3DryRunReissue is T031. It executes the OPEN-3
// procedure end-to-end through the CLI carrier and records the dry-run trace.
func TestWithdrawalAuthzOpen3DryRunReissue(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const (
		callerID     = int64(8301)
		intentID     = "intent-open3-stock"
		holderAuthID = "open3-intent-holder"
		stockAuthID  = "open3-stock-grant"
		stockReqID   = "open3-stock-request"
		newAuthID    = "open3-reissued-grant"
		newReqID     = "open3-reissued-request"
		asset        = "0x1111111111111111111111111111111111111111"
		recipient    = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		sender       = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)

	key, keyRef, err := withdrawal.IssueKey(ctx, pool, callerID, "open3-dryrun")
	if err != nil {
		t.Fatalf("issue issuer key: %v", err)
	}
	env := withdrawalAuthzEnv(dsn)
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(callerID, 10)

	// supplyArgs is one scoped supply invocation by the authorized issuer.
	supplyArgs := func(opID, authID, intent, requestID string) []string {
		return []string{
			"supply",
			"--operation-id", opID,
			"--authorization-id", authID,
			"--api-key", key,
			"--caller-id", strconv.FormatInt(callerID, 10),
			"--chain-id", "31337",
			"--asset", asset,
			"--recipient", recipient,
			"--amount", "100",
			"--operator", "op-open3",
			"--reason", "open3 dry run",
			"--intent-id", intent,
			"--request-id", requestID,
			"--sender", sender,
			"--fee-max-total", "21000",
			"--fee-max-per-gas", "2",
			"--fee-max-priority", "1",
			"--allows-fee-replacement",
		}
	}

	// Fixture: record the business intent on an intent-holder grant, so the
	// re-issue below REUSES it and the distinct intent set cannot grow.
	holderOp := withdrawalAuthzMintID(t, ctx, env)
	if code, _, stderr := withdrawalAuthzRun(ctx, env, supplyArgs(holderOp, holderAuthID, intentID, "request-"+holderAuthID)...); code != 0 {
		t.Fatalf("seed intent holder exit code = %d; stderr=%s", code, stderr)
	}

	// Step 1 — pick a scopeless stock grant: the legacy DSN-trust supply path
	// (no scope flags, no --api-key) writes grant without a scope row.
	stockOp := withdrawalAuthzMintID(t, ctx, env)
	code, stdout, stderr := withdrawalAuthzRun(ctx, env, "supply",
		"--operation-id", stockOp,
		"--authorization-id", stockAuthID,
		"--caller-id", strconv.FormatInt(callerID, 10),
		"--chain-id", "31337",
		"--asset", asset,
		"--recipient", recipient,
		"--amount", "100",
		"--operator", "op-open3",
		"--reason", "open3 stock grant")
	if code != 0 || !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("stock supply = (%d, %q), want 0/supplied; stderr=%s", code, stdout, stderr)
	}
	if n := withdrawalAuthzGrantCount(t, ctx, pool, stockAuthID); n != 1 {
		t.Fatalf("stock grant rows = %d, want 1", n)
	}
	if n := withdrawalAuthzScopeCount(t, ctx, pool, stockAuthID); n != 0 {
		t.Fatalf("stock grant scope rows = %d, want 0 (queryable, never executable)", n)
	}
	open3SeedRequest(t, ctx, pool, stockReqID, callerID, stockAuthID, asset, recipient, "100")
	// Seed one row per 008 table so the "untouched" snapshot is a real content
	// compare, not a vacuous empty set (T030's non-emptiness guard).
	open3SeedNonceFixture(t, ctx, pool, sender)

	// Step 2 — snapshot before the re-issue.
	beforeNonce := open3Snapshot(t, ctx, pool, open3NonceTables)
	for _, table := range open3NonceTables {
		if len(beforeNonce[table]) == 0 {
			t.Fatalf("fixture %s is empty: the no-side-effect proof would be vacuous", table)
		}
	}
	beforeIntents := open3DistinctIntents(t, ctx, pool)
	if len(beforeIntents) != 1 || beforeIntents[0] != intentID {
		t.Fatalf("distinct intent set before = %v, want [%s]", beforeIntents, intentID)
	}
	oldGrantIdent := open3RowIdentity(t, ctx, pool, "withdrawal_authorizations", "authorization_id", stockAuthID)
	oldReqIdent := open3RowIdentity(t, ctx, pool, "withdrawal_requests", "request_id", stockReqID)

	// Step 3 — authorized issuer re-issues: NEW grant id + scope, audit links
	// the old grant/request ids. The stock grant is never rewritten.
	reissueOp := withdrawalAuthzMintID(t, ctx, env)
	reissueArgs := append(supplyArgs(reissueOp, newAuthID, intentID, newReqID),
		"--reissue-from-authorization-id", stockAuthID,
		"--reissue-from-request-id", stockReqID)
	code, stdout, stderr = withdrawalAuthzRun(ctx, env, reissueArgs...)
	if code != 0 {
		t.Fatalf("re-issue exit code = %d, want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "authorization_id="+newAuthID) || !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("re-issue stdout %q lacks the new grant supplied outcome", stdout)
	}

	// Step 4 — trace complete: the audit detail links both old ids.
	action, detail := open3AuditRow(t, ctx, pool, reissueOp)
	if action != "supplied" {
		t.Fatalf("re-issue audit action = %q, want supplied", action)
	}
	for _, want := range []string{
		`reissued_from_authorization_id="` + stockAuthID + `"`,
		`reissued_from_request_id="` + stockReqID + `"`,
	} {
		if !strings.Contains(detail, want) {
			t.Fatalf("re-issue audit detail %q lacks %q", detail, want)
		}
	}

	// Step 5 — the NEW grant passes independent read-back: grant + scope rows
	// agree with the op-input, scope at version 1, principal server-resolved.
	if n := withdrawalAuthzGrantCount(t, ctx, pool, newAuthID); n != 1 {
		t.Fatalf("new grant rows = %d, want 1", n)
	}
	if state, amount := withdrawalAuthzGrantState(t, ctx, pool, newAuthID); state != "active" || amount != "100" {
		t.Fatalf("new grant = (%s, %s), want (active, 100)", state, amount)
	}
	newIntent, newSender, attestedBy, version := open3ScopeRead(t, ctx, pool, newAuthID)
	if newIntent != intentID || newSender != strings.ToLower(sender) || version != 1 {
		t.Fatalf("new scope = (intent=%q sender=%q version=%d), want (%q, %q, 1)",
			newIntent, newSender, version, intentID, strings.ToLower(sender))
	}
	wantAttested := fmt.Sprintf("key:%d/caller:%d", keyRef.KeyID, callerID)
	if attestedBy != wantAttested {
		t.Fatalf("new scope attested_by = %q, want %q", attestedBy, wantAttested)
	}

	// Step 6 — the OLD stock rows are untouched: same physical tuple identity
	// and still scopeless.
	if got := open3RowIdentity(t, ctx, pool, "withdrawal_authorizations", "authorization_id", stockAuthID); got != oldGrantIdent {
		t.Fatalf("old stock grant tuple identity changed: %s -> %s (an UPDATE is forbidden)", oldGrantIdent, got)
	}
	if got := open3RowIdentity(t, ctx, pool, "withdrawal_requests", "request_id", stockReqID); got != oldReqIdent {
		t.Fatalf("old request tuple identity changed: %s -> %s (an UPDATE is forbidden)", oldReqIdent, got)
	}
	if n := withdrawalAuthzScopeCount(t, ctx, pool, stockAuthID); n != 0 {
		t.Fatalf("old stock grant gained a scope row (n=%d); stock stays queryable-but-refused", n)
	}

	// Step 7 — zero new nonce rows (full-content, not count-only) and zero new
	// intent. Every 008 table must be byte-identical; an in-place UPDATE is
	// caught as a removal exactly like an insert is caught as an addition.
	afterNonce := open3Snapshot(t, ctx, pool, open3NonceTables)
	nonceRows := make(map[string]int, len(open3NonceTables))
	for _, table := range open3NonceTables {
		added, removed := open3Diff(beforeNonce[table], afterNonce[table])
		if len(added) != 0 || len(removed) != 0 {
			t.Fatalf("%s drifted across re-issue: added=%v removed=%v", table, added, removed)
		}
		nonceRows[table] = len(afterNonce[table])
	}
	afterIntents := open3DistinctIntents(t, ctx, pool)
	if len(afterIntents) != len(beforeIntents) || afterIntents[0] != intentID {
		t.Fatalf("distinct intent set changed across re-issue: %v -> %v (want unchanged)", beforeIntents, afterIntents)
	}

	t.Logf("OPEN-3 dry-run: stock=%s reissued=%s trace=%q nonce_rows=%v intents=%v",
		stockAuthID, newAuthID, detail, nonceRows, afterIntents)
}
