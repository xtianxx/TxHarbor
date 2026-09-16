//go:build integration

package app

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// withdrawalAuthzEnv is the full serve env pointed at one migrated scratch
// database (reuses fullServeEnv; the T029 harness).
func withdrawalAuthzEnv(dsn string) map[string]string {
	env := fullServeEnv("127.0.0.1:0")
	env[config.EnvPGDSN] = dsn
	env[config.EnvChainID] = "31337"
	return env
}

func withdrawalAuthzRun(ctx context.Context, env map[string]string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := WithdrawalAuthz(ctx, args, Deps{
		Getenv: fakeEnv(env),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	return code, stdout.String(), stderr.String()
}

func withdrawalAuthzSeedCaller(t *testing.T, ctx context.Context, pool *pgxpool.Pool, callerID int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO caller (caller_id, label) VALUES ($1, 'authz-cmd-test')`, callerID); err != nil {
		t.Fatalf("seed caller %d: %v", callerID, err)
	}
}

func withdrawalAuthzMintID(t *testing.T, ctx context.Context, env map[string]string) string {
	t.Helper()
	code, stdout, stderr := withdrawalAuthzRun(ctx, env, "mint")
	if code != 0 {
		t.Fatalf("mint exit code = %d, want 0; stderr=%s", code, stderr)
	}
	id := strings.TrimSpace(stdout)
	if !withdrawalAuthzMintRE.MatchString(id) {
		t.Fatalf("mint stdout = %q, want one 32-hex id", stdout)
	}
	return id
}

func withdrawalAuthzAuditActions(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) []string {
	t.Helper()
	rows, err := pool.Query(ctx,
		`SELECT action FROM withdrawal_grant_audit WHERE authorization_id = $1 ORDER BY audit_id`, authorizationID)
	if err != nil {
		t.Fatalf("query grant audit: %v", err)
	}
	defer rows.Close()
	var actions []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			t.Fatalf("scan grant audit action: %v", err)
		}
		actions = append(actions, a)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate grant audit: %v", err)
	}
	return actions
}

func withdrawalAuthzAuditCountByOp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_grant_audit WHERE operation_id = $1`, operationID).Scan(&n); err != nil {
		t.Fatalf("count audit rows for operation %q: %v", operationID, err)
	}
	return n
}

func withdrawalAuthzGrantCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_authorizations WHERE authorization_id = $1`, authorizationID).Scan(&n); err != nil {
		t.Fatalf("count grant rows for %q: %v", authorizationID, err)
	}
	return n
}

func withdrawalAuthzScopeCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_authorization_scopes WHERE authorization_id = $1`, authorizationID).Scan(&n); err != nil {
		t.Fatalf("count scope rows for %q: %v", authorizationID, err)
	}
	return n
}

func withdrawalAuthzGrantState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, authorizationID string) (string, string) {
	t.Helper()
	var state, amount string
	if err := pool.QueryRow(ctx,
		`SELECT state, amount::text FROM withdrawal_authorizations WHERE authorization_id = $1`, authorizationID).
		Scan(&state, &amount); err != nil {
		t.Fatalf("read grant row for %q: %v", authorizationID, err)
	}
	return state, amount
}

// TestWithdrawalAuthzCmdMintSupplyRevoke drives the T009 carrier in-process
// against a real PostgreSQL: mint → first supply (grant row + 'supplied'
// audit) → same-O same-input (converges, no second row) → same-O different
// input (operation_conflict, exit 1, original intact) → revoke ('revoked') →
// repeat revoke (distinct 'revoke_nop' row).
func TestWithdrawalAuthzCmdMintSupplyRevoke(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()
	env := withdrawalAuthzEnv(dsn)

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()
	withdrawalAuthzSeedCaller(t, ctx, pool, 8001)

	const (
		authID    = "authz-cmd-grant-1"
		asset     = "0x1111111111111111111111111111111111111111"
		recipient = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)
	supplyTail := []string{
		"--authorization-id", authID,
		"--caller-id", "8001",
		"--chain-id", "31337",
		"--asset", asset,
		"--recipient", recipient,
		"--amount", "100",
		"--operator", "op-cmd",
		"--reason", "first supply",
	}
	// runSupply injects the attempt key and any overriding tail flags (the
	// flag package takes the last value, so a tail --amount wins).
	runSupply := func(opID string, tail ...string) (int, string, string) {
		args := append([]string{"supply", "--operation-id", opID}, supplyTail...)
		return withdrawalAuthzRun(ctx, env, append(args, tail...)...)
	}

	o1 := withdrawalAuthzMintID(t, ctx, env)

	code, stdout, stderr := runSupply(o1)
	if code != 0 {
		t.Fatalf("first supply exit code = %d, want 0; stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "authorization_id="+authID) || !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("first supply stdout %q lacks the supplied outcome", stdout)
	}
	if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
	if state, amount := withdrawalAuthzGrantState(t, ctx, pool, authID); state != "active" || amount != "100" {
		t.Fatalf("grant state/amount = %q/%q, want active/100", state, amount)
	}
	if got := withdrawalAuthzAuditActions(t, ctx, pool, authID); len(got) != 1 || got[0] != "supplied" {
		t.Fatalf("audit actions = %v, want [supplied]", got)
	}

	// Same O + same op-input converges on the recorded outcome, no second row.
	code, stdout, stderr = runSupply(o1)
	if code != 0 {
		t.Fatalf("same-O re-supply exit code = %d, want 0; stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "action=supplied") {
		t.Fatalf("same-O re-supply stdout %q, want the recorded supplied outcome", stdout)
	}
	if n := withdrawalAuthzAuditCountByOp(t, ctx, pool, o1); n != 1 {
		t.Fatalf("audit rows for O = %d, want 1 (same attempt converges)", n)
	}
	if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows after re-supply = %d, want 1", n)
	}

	// Same O + different op-input → operation_conflict, exit 1, original intact.
	code, _, stderr = runSupply(o1, "--amount", "200")
	if code != 1 {
		t.Fatalf("same-O different-input exit code = %d, want 1; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "operation_conflict") {
		t.Fatalf("stderr %q lacks operation_conflict", stderr)
	}
	if state, amount := withdrawalAuthzGrantState(t, ctx, pool, authID); state != "active" || amount != "100" {
		t.Fatalf("grant state/amount after conflict = %q/%q, want active/100 (original intact)", state, amount)
	}
	if n := withdrawalAuthzAuditCountByOp(t, ctx, pool, o1); n != 1 {
		t.Fatalf("audit rows for O after conflict = %d, want 1", n)
	}

	// Revoke with a fresh O → exit 0, 'revoked'.
	o2 := withdrawalAuthzMintID(t, ctx, env)
	code, stdout, stderr = withdrawalAuthzRun(ctx, env, "revoke",
		"--operation-id", o2, "--authorization-id", authID, "--operator", "op-cmd", "--reason", "revoke")
	if code != 0 {
		t.Fatalf("revoke exit code = %d, want 0; stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "action=revoked") {
		t.Fatalf("revoke stdout %q, want action=revoked", stdout)
	}
	if state, _ := withdrawalAuthzGrantState(t, ctx, pool, authID); state != "revoked" {
		t.Fatalf("grant state after revoke = %q, want revoked", state)
	}

	// Repeat revoke with a fresh O → exit 0, its own distinct 'revoke_nop' row.
	o3 := withdrawalAuthzMintID(t, ctx, env)
	code, stdout, stderr = withdrawalAuthzRun(ctx, env, "revoke",
		"--operation-id", o3, "--authorization-id", authID, "--operator", "op-cmd", "--reason", "repeat revoke")
	if code != 0 {
		t.Fatalf("repeat revoke exit code = %d, want 0; stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "action=revoke_nop") {
		t.Fatalf("repeat revoke stdout %q, want action=revoke_nop", stdout)
	}
	if n := withdrawalAuthzAuditCountByOp(t, ctx, pool, o3); n != 1 {
		t.Fatalf("audit rows for repeat-revoke O = %d, want 1 (distinct row)", n)
	}
	if got := withdrawalAuthzAuditActions(t, ctx, pool, authID); len(got) != 3 ||
		got[0] != "supplied" || got[1] != "revoked" || got[2] != "revoke_nop" {
		t.Fatalf("audit actions = %v, want [supplied revoked revoke_nop]", got)
	}
}

// TestWithdrawalAuthzCmdPermissionGate is the T011 fail-first test of the
// issuance permission gate (R-PB9/R-PB10; contracts/supply-scope.md
// preconditions 1-2). The supply carrier must resolve the presented API key
// through Authenticate to its caller_id and then consult PermitIssue against
// TXHARBOR_AUTHZ_ISSUER_CALLERS: an authenticated but unmapped principal is
// refused with zero rows, while a mapped principal supplies green.
//
// RED until T013 wires Authenticate -> PermitIssue -> SupplyGrant into the
// carrier: today `--api-key` is not a supply flag (usage error, exit 2) and no
// permission predicate is consulted, so the unmapped supply would otherwise
// commit a grant row.
func TestWithdrawalAuthzCmdPermissionGate(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const (
		unmappedCaller = int64(8101)
		mappedCaller   = int64(8102)
		asset          = "0x1111111111111111111111111111111111111111"
		recipient      = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	)

	// Both principals hold a valid credential — the difference under test is
	// the deployment mapping, not key validity. IssueKey creates the caller
	// rows the grant FK needs.
	unmappedKey, _, err := withdrawal.IssueKey(ctx, pool, unmappedCaller, "authz-gate-unmapped")
	if err != nil {
		t.Fatalf("issue unmapped caller key: %v", err)
	}
	mappedKey, _, err := withdrawal.IssueKey(ctx, pool, mappedCaller, "authz-gate-mapped")
	if err != nil {
		t.Fatalf("issue mapped caller key: %v", err)
	}

	// Only the mapped caller is an authorized issuer: the unmapped principal
	// authenticates yet is absent from the mapping -> deny-by-default.
	env := withdrawalAuthzEnv(dsn)
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(mappedCaller, 10)

	supplyArgs := func(opID, authID, apiKey string, callerID int64) []string {
		return []string{
			"supply",
			"--operation-id", opID,
			"--authorization-id", authID,
			"--api-key", apiKey,
			"--caller-id", strconv.FormatInt(callerID, 10),
			"--chain-id", "31337",
			"--asset", asset,
			"--recipient", recipient,
			"--amount", "100",
			"--operator", "op-gate",
			"--reason", "permission gate",
		}
	}

	t.Run("unmapped principal refused with zero rows", func(t *testing.T) {
		const authID = "authz-gate-unmapped"
		o := withdrawalAuthzMintID(t, ctx, env)
		code, stdout, stderr := withdrawalAuthzRun(ctx, env, supplyArgs(o, authID, unmappedKey, unmappedCaller)...)
		if code != 1 {
			t.Fatalf("unmapped supply exit code = %d, want 1 (refusal); stdout=%s stderr=%s", code, stdout, stderr)
		}
		if strings.Contains(stdout, "action=supplied") {
			t.Fatalf("unmapped supply stdout %q reports an accepted supply", stdout)
		}
		if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 0 {
			t.Fatalf("grant rows after unmapped supply = %d, want 0", n)
		}
		if n := len(withdrawalAuthzAuditActions(t, ctx, pool, authID)); n != 0 {
			t.Fatalf("audit rows after unmapped supply = %d, want 0", n)
		}
	})

	t.Run("mapped principal supplies green", func(t *testing.T) {
		const authID = "authz-gate-mapped"
		o := withdrawalAuthzMintID(t, ctx, env)
		code, stdout, stderr := withdrawalAuthzRun(ctx, env, supplyArgs(o, authID, mappedKey, mappedCaller)...)
		if code != 0 {
			t.Fatalf("mapped supply exit code = %d, want 0; stdout=%s stderr=%s", code, stdout, stderr)
		}
		if !strings.Contains(stdout, "action=supplied") {
			t.Fatalf("mapped supply stdout %q lacks action=supplied", stdout)
		}
		if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 1 {
			t.Fatalf("grant rows after mapped supply = %d, want 1", n)
		}
		if got := withdrawalAuthzAuditActions(t, ctx, pool, authID); len(got) != 1 || got[0] != "supplied" {
			t.Fatalf("audit actions after mapped supply = %v, want [supplied]", got)
		}
	})
}

// TestWithdrawalAuthzCmdNegativeMatrix is T020: the refusal paths of the
// extended supply entry write nothing. An unauthenticated scoped supply and a
// scoped supply with no key are refused; a malformed scope payload and an
// over-limit fee triple are refused by the library before any pool write. Every
// case asserts ZERO grant rows, ZERO scope rows and ZERO audit rows, so a
// refusal can never leave a partial authorization.
func TestWithdrawalAuthzCmdNegativeMatrix(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const (
		mappedCaller = int64(8201)
		asset        = "0x1111111111111111111111111111111111111111"
		recipient    = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		sender       = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	key, _, err := withdrawal.IssueKey(ctx, pool, mappedCaller, "authz-negative-matrix")
	if err != nil {
		t.Fatalf("issue mapped key: %v", err)
	}

	env := withdrawalAuthzEnv(dsn)
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(mappedCaller, 10)

	scopedSupplyArgs := func(opID, authID, apiKey, senderAddr, total, perGas, priority string) []string {
		args := []string{
			"supply",
			"--operation-id", opID,
			"--authorization-id", authID,
			"--caller-id", strconv.FormatInt(mappedCaller, 10),
			"--chain-id", "31337",
			"--asset", asset,
			"--recipient", recipient,
			"--amount", "100",
			"--operator", "op-matrix",
			"--reason", "negative matrix",
			"--intent-id", "intent-" + authID,
			"--request-id", "request-" + authID,
			"--sender", senderAddr,
			"--fee-max-total", total,
			"--fee-max-per-gas", perGas,
			"--fee-max-priority", priority,
			"--allows-fee-replacement",
		}
		if apiKey != "" {
			args = append(args, "--api-key", apiKey)
		}
		return args
	}

	cases := []struct {
		name       string
		args       func(opID, authID string) []string
		wantExit   int
		wantStderr string
	}{
		{
			"unauthenticated scoped supply",
			func(opID, authID string) []string {
				return scopedSupplyArgs(opID, authID,
					"0000000000000000000000000000000000000000000000000000000000000000",
					sender, "21000", "2", "1")
			},
			1, "",
		},
		{
			"scoped supply without api key is a usage error",
			func(opID, authID string) []string {
				return scopedSupplyArgs(opID, authID, "", sender, "21000", "2", "1")
			},
			2, "--api-key is required for a scoped supply",
		},
		{
			"malformed scope sender",
			func(opID, authID string) []string {
				return scopedSupplyArgs(opID, authID, key, "0x1234", "21000", "2", "1")
			},
			1, "sender",
		},
		{
			"over-limit priority fee",
			func(opID, authID string) []string {
				return scopedSupplyArgs(opID, authID, key, sender, "21000", "2", "3")
			},
			1, "fee_max_priority",
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			authID := "authz-negative-" + strconv.Itoa(i)
			opID := withdrawalAuthzMintID(t, ctx, env)
			code, stdout, stderr := withdrawalAuthzRun(ctx, env, tc.args(opID, authID)...)
			if code != tc.wantExit {
				t.Fatalf("exit code = %d, want %d; stdout=%s stderr=%s", code, tc.wantExit, stdout, stderr)
			}
			if strings.Contains(stdout, "action=supplied") {
				t.Fatalf("stdout %q reports an accepted supply on a refusal path", stdout)
			}
			if tc.wantStderr != "" && !strings.Contains(stderr, tc.wantStderr) {
				t.Fatalf("stderr %q does not name %q", stderr, tc.wantStderr)
			}
			if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 0 {
				t.Fatalf("grant rows = %d, want 0", n)
			}
			if n := withdrawalAuthzScopeCount(t, ctx, pool, authID); n != 0 {
				t.Fatalf("scope rows = %d, want 0", n)
			}
			if got := withdrawalAuthzAuditActions(t, ctx, pool, authID); len(got) != 0 {
				t.Fatalf("audit actions = %v, want none", got)
			}
			if n := withdrawalAuthzAuditCountByOp(t, ctx, pool, opID); n != 0 {
				t.Fatalf("audit rows for operation %q = %d, want 0", opID, n)
			}
		})
	}
}
