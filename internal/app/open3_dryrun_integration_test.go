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

// open3NonceRowCounts counts the rows of every 008 nonce table. Zero before and
// after the re-issue is the "zero new intent/nonce" proof: any intent-binding
// write (nonce_bindings.intent_id) would make a count non-zero.
func open3NonceRowCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(open3NonceTables))
	for _, table := range open3NonceTables {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		counts[table] = n
	}
	return counts
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

	// Step 2 — snapshot before the re-issue.
	beforeNonce := open3NonceRowCounts(t, ctx, pool)
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

	// Step 7 — zero new nonce and zero new intent.
	afterNonce := open3NonceRowCounts(t, ctx, pool)
	for _, table := range open3NonceTables {
		if afterNonce[table] != beforeNonce[table] {
			t.Fatalf("%s rows changed across re-issue: %d -> %d", table, beforeNonce[table], afterNonce[table])
		}
	}
	afterIntents := open3DistinctIntents(t, ctx, pool)
	if len(afterIntents) != len(beforeIntents) || afterIntents[0] != intentID {
		t.Fatalf("distinct intent set changed across re-issue: %v -> %v (want unchanged)", beforeIntents, afterIntents)
	}

	t.Logf("OPEN-3 dry-run: stock=%s reissued=%s trace=%q nonce_rows=%v intents=%v",
		stockAuthID, newAuthID, detail, afterNonce, afterIntents)
}
