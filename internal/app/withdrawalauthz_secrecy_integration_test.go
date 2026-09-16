//go:build integration

// withdrawalauthz_secrecy_integration_test.go is the end-to-end T014 credential
// secrecy proof: a REAL issued API key is presented to the supply carrier and
// the supply commits, yet the plaintext never reaches CLI stdout/stderr, any
// slog record, or the persisted audit detail. Reuses the T029/T011 app harness
// (startConfirmAuthPostgres, withdrawalAuthzEnv/Run/MintID, grant counters)
// rather than adding a second one.
package app

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// TestWithdrawalAuthzSupplyCredentialSecrecy is RED until T013 wires the
// `--api-key` flag + Authenticate -> PermitIssue -> SupplyGrant path into the
// carrier: today `--api-key` is an unknown flag (exit 2) and no supply commits,
// so the acceptance assertion fails first. Once green it proves the secret is
// consumed by the real authentication path and still never logged or stored.
func TestWithdrawalAuthzSupplyCredentialSecrecy(t *testing.T) {
	dsn := startConfirmAuthPostgres(t)
	ctx := context.Background()

	pool, err := db.OpenPool(ctx, dsn, 5*time.Second)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	defer pool.Close()

	const (
		callerID = int64(8201)
		authID   = "authz-secrecy-grant"
	)
	// IssueKey creates the caller row the grant FK needs and returns the
	// plaintext exactly once — a genuine credential, not a shape stub.
	plaintext, _, err := withdrawal.IssueKey(ctx, pool, callerID, "authz-secrecy")
	if err != nil {
		t.Fatalf("issue key: %v", err)
	}

	// The caller is an authorized issuer, so the presented key is actually
	// resolved and consumed (not refused on shape or permission).
	env := withdrawalAuthzEnv(dsn)
	env[withdrawal.EnvIssuerCallers] = strconv.FormatInt(callerID, 10)

	origLog := slog.Default()
	var slogBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&slogBuf, nil)))
	defer slog.SetDefault(origLog)

	// The credential travels in a restricted file, not argv: the end-to-end
	// proof covers the preferred input form, so the secret never enters the
	// process arguments at all.
	keyPath := filepath.Join(t.TempDir(), "api-key")
	if err := os.WriteFile(keyPath, []byte(plaintext+"\n"), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	opID := withdrawalAuthzMintID(t, ctx, env)
	code, stdout, stderr := withdrawalAuthzRun(ctx, env, "supply",
		"--operation-id", opID,
		"--authorization-id", authID,
		"--api-key-file", keyPath,
		"--caller-id", strconv.FormatInt(callerID, 10),
		"--chain-id", "31337",
		"--asset", "0x1111111111111111111111111111111111111111",
		"--recipient", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--amount", "100",
		"--operator", "op-secrecy",
		"--reason", "credential secrecy",
	)
	if code != 0 {
		t.Fatalf("supply with a valid key exit code = %d, want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}

	// The presented plaintext must appear in no observable output channel.
	for _, out := range []struct{ name, text string }{
		{"stdout", stdout},
		{"stderr", stderr},
		{"slog capture", slogBuf.String()},
	} {
		if strings.Contains(out.text, plaintext) {
			t.Fatalf("presented API key leaked into %s: %q", out.name, out.text)
		}
	}

	// Nor may it be persisted: the audit detail snapshot carries only
	// non-secret fields (the api_key table stores the digest elsewhere).
	var leaks int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM withdrawal_grant_audit WHERE position($1 in detail) > 0`, plaintext).Scan(&leaks); err != nil {
		t.Fatalf("scan audit for key leak: %v", err)
	}
	if leaks != 0 {
		t.Fatalf("presented API key stored in %d audit detail row(s)", leaks)
	}

	// The committed grant proves the authenticated path actually ran.
	if n := withdrawalAuthzGrantCount(t, ctx, pool, authID); n != 1 {
		t.Fatalf("grant rows = %d, want 1", n)
	}
}
