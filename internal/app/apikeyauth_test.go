package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

// apiKeyAuthFullIssue/Rotate/Revoke are the minimal valid flag shapes; the
// usage matrix mutates them one flag at a time.
func apiKeyAuthFullIssue() []string {
	return []string{"issue", "--caller-id", "7", "--label", "life", "--operator", "op", "--reason", "bootstrap"}
}

func apiKeyAuthFullRotate() []string {
	return []string{"rotate", "--caller-id", "7", "--operator", "op", "--reason", "rotate"}
}

func apiKeyAuthFullRevoke() []string {
	return []string{"revoke", "--key-id", "9", "--operator", "op", "--reason", "retire"}
}

// TestAPIKeyAuthUsageErrors pins exit code 2 for every flag-shape failure: no
// args, an unknown action (apikey-auth has no mint), each missing required
// flag, non-integer/zero/negative ids, a negative grace window, an unknown
// flag, and trailing positionals. None of these reach config or the DB.
func TestAPIKeyAuthUsageErrors(t *testing.T) {
	issue, rotate, revoke := apiKeyAuthFullIssue(), apiKeyAuthFullRotate(), apiKeyAuthFullRevoke()
	cases := []struct {
		name string
		args []string
	}{
		{name: "no args", args: nil},
		{name: "unknown action mint", args: []string{"mint", "--caller-id", "7"}},
		{name: "unknown action", args: []string{"rotate-key"}},

		{name: "issue missing caller-id", args: dropFlag(issue, "--caller-id")},
		{name: "issue missing label", args: dropFlag(issue, "--label")},
		{name: "issue missing operator", args: dropFlag(issue, "--operator")},
		{name: "issue missing reason", args: dropFlag(issue, "--reason")},
		{name: "issue non-integer caller-id", args: replaceFlag(issue, "--caller-id", "abc")},
		{name: "issue zero caller-id", args: replaceFlag(issue, "--caller-id", "0")},
		{name: "issue negative caller-id", args: replaceFlag(issue, "--caller-id", "-3")},
		{name: "issue unknown flag", args: append(append([]string{}, issue...), "--nope", "x")},
		{name: "issue positional", args: append(append([]string{}, issue...), "extra")},

		{name: "rotate missing caller-id", args: dropFlag(rotate, "--caller-id")},
		{name: "rotate missing operator", args: dropFlag(rotate, "--operator")},
		{name: "rotate missing reason", args: dropFlag(rotate, "--reason")},
		{name: "rotate non-integer caller-id", args: replaceFlag(rotate, "--caller-id", "abc")},
		{name: "rotate negative caller-id", args: replaceFlag(rotate, "--caller-id", "-1")},
		{name: "rotate non-integer grace", args: append(append([]string{}, rotate...), "--grace-seconds", "abc")},
		{name: "rotate negative grace", args: append(append([]string{}, rotate...), "--grace-seconds", "-5")},
		{name: "rotate positional", args: append(append([]string{}, rotate...), "extra")},

		{name: "revoke missing key-id", args: dropFlag(revoke, "--key-id")},
		{name: "revoke missing operator", args: dropFlag(revoke, "--operator")},
		{name: "revoke missing reason", args: dropFlag(revoke, "--reason")},
		{name: "revoke non-integer key-id", args: replaceFlag(revoke, "--key-id", "abc")},
		{name: "revoke zero key-id", args: replaceFlag(revoke, "--key-id", "0")},
		{name: "revoke negative key-id", args: replaceFlag(revoke, "--key-id", "-9")},
		{name: "revoke positional", args: append(append([]string{}, revoke...), "extra")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := APIKeyAuth(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 2 {
				t.Fatalf("APIKeyAuth() exit code = %d, want 2; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: txharbor apikey-auth") {
				t.Errorf("stderr %q lacks the usage line", stderr.String())
			}
		})
	}
}

// TestAPIKeyAuthConfigError pins exit code 1 (redacted) when the serve env is
// invalid: a valid flag shape and empty env, so config.Load fails before any
// DB dial (and before any secret is minted).
func TestAPIKeyAuthConfigError(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "issue", args: apiKeyAuthFullIssue()},
		{name: "rotate", args: apiKeyAuthFullRotate()},
		{name: "revoke", args: apiKeyAuthFullRevoke()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := APIKeyAuth(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(map[string]string{}),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 1 {
				t.Fatalf("APIKeyAuth() exit code = %d, want 1; stderr=%s", code, stderr.String())
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
