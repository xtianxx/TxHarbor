package app

import (
	"bytes"
	"context"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

// withdrawalAuthzMintRE pins the mint contract: 32 lowercase hex characters.
var withdrawalAuthzMintRE = regexp.MustCompile(`^[0-9a-f]{32}$`)

// withdrawalAuthzSupplyArgs is a fully valid supply invocation with a
// placeholder operation id; the usage matrix mutates one field at a time.
func withdrawalAuthzSupplyArgs() []string {
	return []string{
		"supply",
		"--operation-id", "0123456789abcdef0123456789abcdef",
		"--authorization-id", "grant-1",
		"--caller-id", "1",
		"--chain-id", "31337",
		"--asset", "0x1111111111111111111111111111111111111111",
		"--recipient", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"--amount", "100",
		"--operator", "op",
		"--reason", "r",
	}
}

func withdrawalAuthzRevokeArgs() []string {
	return []string{
		"revoke",
		"--operation-id", "0123456789abcdef0123456789abcdef",
		"--authorization-id", "grant-1",
		"--operator", "op",
		"--reason", "r",
	}
}

// TestWithdrawalAuthzUsageErrors pins exit code 2 for every flag-shape failure:
// no args, an unknown action, a missing --operation-id on supply and revoke, a
// malformed --expires-at, and trailing positionals. None of these reach config
// or the DB (the full serve env is present only to prove the code path stops
// before it would ever be read).
func TestWithdrawalAuthzUsageErrors(t *testing.T) {
	supply := withdrawalAuthzSupplyArgs()
	revoke := withdrawalAuthzRevokeArgs()
	cases := []struct {
		name string
		args []string
	}{
		{name: "no args", args: nil},
		{name: "unknown action", args: []string{"frobnicate"}},
		{name: "supply missing operation-id", args: dropFlag(supply, "--operation-id")},
		{name: "revoke missing operation-id", args: dropFlag(revoke, "--operation-id")},
		{name: "supply empty operation-id", args: replaceFlag(supply, "--operation-id", "")},
		{name: "malformed expires-at", args: append(append([]string{}, supply...), "--expires-at", "not-a-time")},
		{name: "supply trailing positional", args: append(append([]string{}, supply...), "extra")},
		{name: "revoke trailing positional", args: append(append([]string{}, revoke...), "extra")},
		{name: "mint extra arg", args: []string{"mint", "extra"}},
		{name: "unknown flag", args: append(append([]string{}, supply...), "--nope", "x")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 2 {
				t.Fatalf("WithdrawalAuthz() exit code = %d, want 2; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: txharbor withdrawal-authz") {
				t.Errorf("stderr %q lacks the usage line", stderr.String())
			}
		})
	}
}

// TestWithdrawalAuthzMissingOperationIDPrecedesConfig locks the R9 rule that a
// missing --operation-id is a usage error with ZERO side effects: with an empty
// environment the missing-id path still returns 2 (not the configuration
// error 1), so it never reaches config.Load or a database.
func TestWithdrawalAuthzMissingOperationIDPrecedesConfig(t *testing.T) {
	for _, args := range [][]string{
		dropFlag(withdrawalAuthzSupplyArgs(), "--operation-id"),
		dropFlag(withdrawalAuthzRevokeArgs(), "--operation-id"),
	} {
		var stdout, stderr bytes.Buffer
		code := WithdrawalAuthz(context.Background(), args, Deps{
			Getenv: fakeEnv(map[string]string{}),
			Stdout: &stdout,
			Stderr: &stderr,
		})
		if code != 2 {
			t.Fatalf("missing --operation-id exit code = %d, want 2; stderr=%s", code, stderr.String())
		}
	}
}

// TestWithdrawalAuthzMintPrintsOneID pins mint: one 32-hex id on stdout, exit
// 0. mint is pure entropy (R9 "standalone"), so it also succeeds with an empty
// environment and never dials a database.
func TestWithdrawalAuthzMintPrintsOneID(t *testing.T) {
	envs := map[string]map[string]string{
		"valid serve env": fullServeEnv("127.0.0.1:0"),
		"empty env":       {},
	}
	for name, env := range envs {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), []string{"mint"}, Deps{
				Getenv: fakeEnv(env),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 0 {
				t.Fatalf("mint exit code = %d, want 0; stderr=%s", code, stderr.String())
			}
			id := strings.TrimSpace(stdout.String())
			if !withdrawalAuthzMintRE.MatchString(id) {
				t.Fatalf("mint stdout = %q, want one 32-hex id", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Errorf("mint stderr = %q, want empty", stderr.String())
			}

			var second bytes.Buffer
			code = WithdrawalAuthz(context.Background(), []string{"mint"}, Deps{
				Getenv: fakeEnv(env),
				Stdout: &second,
				Stderr: &stderr,
			})
			if code != 0 {
				t.Fatalf("second mint exit code = %d, want 0; stderr=%s", code, stderr.String())
			}
			if strings.TrimSpace(second.String()) == id {
				t.Errorf("two mints returned the same id %q", id)
			}
		})
	}
}

// TestWithdrawalAuthzConfigError pins exit code 1 (redacted) when the serve env
// is invalid: valid flags, empty env, so config.Load fails before any DB dial.
func TestWithdrawalAuthzConfigError(t *testing.T) {
	for name, args := range map[string][]string{
		"supply": withdrawalAuthzSupplyArgs(),
		"revoke": withdrawalAuthzRevokeArgs(),
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), args, Deps{
				Getenv: fakeEnv(map[string]string{}),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 1 {
				t.Fatalf("exit code = %d, want 1; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "configuration error") {
				t.Errorf("stderr %q lacks the configuration error", stderr.String())
			}
			if !strings.Contains(stderr.String(), config.EnvPGDSN) {
				t.Errorf("stderr %q does not name %s", stderr.String(), config.EnvPGDSN)
			}
		})
	}
}

// TestWithdrawalAuthzChainBindRefused pins FR-04: a supply whose --chain-id
// differs from the deployment chain is refused with exit 1 before any pool is
// opened (the env DSN points at 127.0.0.1:0, where a dial would fail as exit 1
// too, but the failure here names the chain bind).
func TestWithdrawalAuthzChainBindRefused(t *testing.T) {
	args := replaceFlag(withdrawalAuthzSupplyArgs(), "--chain-id", "1")
	var stdout, stderr bytes.Buffer
	code := WithdrawalAuthz(context.Background(), args, Deps{
		Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "chain_id") || !strings.Contains(stderr.String(), "does not match") {
		t.Errorf("stderr %q does not report the chain bind refusal", stderr.String())
	}
}

// TestWithdrawalAuthzFeeParseNativeIntegerBoundaries pins the fee-cap integer
// domain at the carrier boundary (PB-C2: native-coin smallest-unit integers):
// an unset flag is 0, the largest native integer is accepted unchanged, and a
// cap+1 that overflows int64 is a usage error (exit 2) rather than a silently
// truncated fee. Pre-pool: withdrawalAuthzFee touches no database.
func TestWithdrawalAuthzFeeParseNativeIntegerBoundaries(t *testing.T) {
	maxInt64 := strconv.FormatInt(math.MaxInt64, 10)
	for _, raw := range []string{"0", "1", "21000", maxInt64} {
		got, code := withdrawalAuthzFee(io.Discard, "--fee-max-total", raw)
		if code != 0 {
			t.Fatalf("withdrawalAuthzFee(%q) code = %d, want 0", raw, code)
		}
		want, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("test parse %q: %v", raw, err)
		}
		if got != want {
			t.Fatalf("withdrawalAuthzFee(%q) = %d, want %d", raw, got, want)
		}
	}
	if got, code := withdrawalAuthzFee(io.Discard, "--fee-max-total", ""); code != 0 || got != 0 {
		t.Fatalf("withdrawalAuthzFee(\"\") = (%d, %d), want (0, 0)", got, code)
	}
	for _, raw := range []string{"9223372036854775808", "1.5", "0x10", "twelve"} {
		var stderr bytes.Buffer
		if _, code := withdrawalAuthzFee(&stderr, "--fee-max-total", raw); code != 2 {
			t.Fatalf("withdrawalAuthzFee(%q) code = %d, want 2 (usage error)", raw, code)
		}
		if !strings.Contains(stderr.String(), "--fee-max-total") {
			t.Fatalf("stderr %q does not name the flag", stderr.String())
		}
	}
}
