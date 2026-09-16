package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

const testSender = "0x00000000000000000000000000000000000000aa"

func emptyEnv(string) (string, bool) { return "", false }

func nonceAdminDeps(stdout, stderr *bytes.Buffer) Deps {
	return Deps{Getenv: emptyEnv, Stdout: stdout, Stderr: stderr}
}

// TestNonceAdminUsageAndExitCodes pins the carrier's usage boundary (exit 2)
// with zero configuration/database access: every case runs against an env that
// would fail config.Load, so a code other than 2 means the usage gate leaked.
func TestNonceAdminUsageAndExitCodes(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantCode int
		wantErr  string
	}{
		{name: "no action", args: nil, wantCode: 2, wantErr: "usage: txharbor nonce-admin"},
		{name: "unknown action", args: []string{"frobnicate"}, wantCode: 2, wantErr: `unknown action "frobnicate"`},
		{name: "mint rejects extra args", args: []string{"mint", "extra"}, wantCode: 2, wantErr: "usage:"},
		{name: "mint rejects flags", args: []string{"mint", "--chain-id", "1"}, wantCode: 2, wantErr: "usage:"},
		{
			name:     "hold-release missing operation id",
			args:     []string{"hold-release", "--hold-id", "h1", "--chain-id", "1", "--sender", testSender},
			wantCode: 2, wantErr: "usage:",
		},
		{
			name:     "hold-release missing hold id",
			args:     []string{"hold-release", "--operation-id", "o1", "--chain-id", "1", "--sender", testSender},
			wantCode: 2, wantErr: "usage:",
		},
		{
			name:     "hold-release malformed chain id",
			args:     []string{"hold-release", "--operation-id", "o1", "--hold-id", "h1", "--chain-id", "abc", "--sender", testSender},
			wantCode: 2, wantErr: "invalid --chain-id",
		},
		{
			name:     "hold-release zero chain id",
			args:     []string{"hold-release", "--operation-id", "o1", "--hold-id", "h1", "--chain-id", "0", "--sender", testSender},
			wantCode: 2, wantErr: "invalid --chain-id",
		},
		{
			name:     "hold-release uppercase sender",
			args:     []string{"hold-release", "--operation-id", "o1", "--hold-id", "h1", "--chain-id", "1", "--sender", strings.ToUpper(testSender)},
			wantCode: 2, wantErr: "invalid --sender",
		},
		{
			name:     "hold-release positional junk",
			args:     []string{"hold-release", "--operation-id", "o1", "--hold-id", "h1", "--chain-id", "1", "--sender", testSender, "junk"},
			wantCode: 2, wantErr: "usage:",
		},
		{
			name:     "binding-release missing operation id",
			args:     []string{"binding-release", "--binding-id", "b1", "--chain-id", "1", "--sender", testSender},
			wantCode: 2, wantErr: "usage:",
		},
		{
			name:     "binding-release missing binding id",
			args:     []string{"binding-release", "--operation-id", "o1", "--chain-id", "1", "--sender", testSender},
			wantCode: 2, wantErr: "usage:",
		},
		{
			name:     "register missing operation id",
			args:     []string{"register", "--chain-id", "1", "--sender", testSender},
			wantCode: 2, wantErr: "usage:",
		},
		{
			name:     "disable missing chain id",
			args:     []string{"disable", "--operation-id", "o1", "--sender", testSender},
			wantCode: 2, wantErr: "usage:",
		},
		{name: "status missing chain id", args: []string{"status"}, wantCode: 2, wantErr: "usage:"},
		{name: "status unknown flag", args: []string{"status", "--chain-id", "1", "--bogus"}, wantCode: 2, wantErr: "usage:"},
		{name: "status malformed sender", args: []string{"status", "--chain-id", "1", "--sender", "0xNOPE"}, wantCode: 2, wantErr: "invalid --sender"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := NonceAdmin(context.Background(), tc.args, nonceAdminDeps(&stdout, &stderr))
			if code != tc.wantCode {
				t.Fatalf("NonceAdmin(%v) = %d, want %d (stderr %q)", tc.args, code, tc.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantErr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), tc.wantErr)
			}
			if stdout.Len() != 0 {
				t.Fatalf("usage path wrote to stdout: %q", stdout.String())
			}
		})
	}
}

// TestNonceAdminMintIsPureEntropy proves `mint` reaches neither config.Load
// nor any connection: the env is empty, so exit 1 would mean a config touch.
func TestNonceAdminMintIsPureEntropy(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := NonceAdmin(context.Background(), []string{"mint"}, nonceAdminDeps(&stdout, &stderr))
	if code != 0 {
		t.Fatalf("mint = %d, want 0 (stderr %q)", code, stderr.String())
	}
	raw := strings.TrimSpace(stdout.String())
	if len(raw) != 32 || raw != strings.ToLower(raw) {
		t.Fatalf("mint output %q is not 32 lowercase hex chars", raw)
	}
	if _, err := hex.DecodeString(raw); err != nil {
		t.Fatalf("mint output %q is not hex: %v", raw, err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("mint wrote to stderr: %q", stderr.String())
	}
}

// TestNonceAdminConfigFailureExitOne pins exit 1 for a well-formed attempt
// whose configuration cannot load: the usage gate passed, the carrier failed.
func TestNonceAdminConfigFailureExitOne(t *testing.T) {
	cases := [][]string{
		{"hold-release", "--operation-id", "o1", "--hold-id", "h1", "--chain-id", "1", "--sender", testSender},
		{"binding-release", "--operation-id", "o1", "--binding-id", "b1", "--chain-id", "1", "--sender", testSender},
		{"register", "--operation-id", "o1", "--chain-id", "1", "--sender", testSender},
		{"disable", "--operation-id", "o1", "--chain-id", "1", "--sender", testSender},
		{"status", "--chain-id", "1"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		code := NonceAdmin(context.Background(), args, nonceAdminDeps(&stdout, &stderr))
		if code != 1 {
			t.Fatalf("NonceAdmin(%v) = %d, want 1 (stderr %q)", args, code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "configuration error") {
			t.Fatalf("stderr %q does not name a configuration error", stderr.String())
		}
	}
}

// TestNonceAdminChainBindBeforeConnect pins the FR-04-style bind: a foreign
// --chain-id is refused with exit 1 after config.Load and BEFORE any RPC or
// database connection (nothing listens on the configured endpoints).
func TestNonceAdminChainBindBeforeConnect(t *testing.T) {
	env := map[string]string{
		config.EnvPGDSN:                 "postgres://op:secret@127.0.0.1:1/nonce",
		config.EnvRPCURL:                "http://127.0.0.1:1",
		config.EnvChainID:               "8453",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x00000000000000000000000000000000000000bb",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x00000000000000000000000000000000000000bb",
		config.EnvDepositWatchAddresses: "0x00000000000000000000000000000000000000bb",
		config.EnvConfirmationDepth:     "12",
	}
	deps := Deps{
		Getenv: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
	}
	var stdout, stderr bytes.Buffer
	deps.Stdout, deps.Stderr = &stdout, &stderr

	args := []string{"hold-release", "--operation-id", "o1", "--hold-id", "h1", "--chain-id", "999", "--sender", testSender, "--observation-id", "ob1", "--evidence", "f", "--operator", "op", "--reason", "r"}
	if code := NonceAdmin(context.Background(), args, deps); code != 1 {
		t.Fatalf("foreign chain = %d, want 1 (stderr %q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "does not match deployment chain 8453") {
		t.Fatalf("stderr %q does not name the chain mismatch", stderr.String())
	}
	if strings.Contains(stderr.String(), "secret") {
		t.Fatalf("stderr leaked the DSN credential: %q", stderr.String())
	}

	// The matching chain passes the bind and reaches the (unreachable)
	// database probe: still exit 1, but no longer the bind refusal.
	stdout.Reset()
	stderr.Reset()
	args[6] = "8453"
	if code := NonceAdmin(context.Background(), args, deps); code != 1 {
		t.Fatalf("matching chain = %d, want 1 from the closed endpoint (stderr %q)", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "does not match deployment chain") {
		t.Fatalf("matching chain was refused by the bind: %q", stderr.String())
	}
}
