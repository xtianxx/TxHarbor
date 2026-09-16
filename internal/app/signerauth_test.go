package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func signerAuthFullIssue() []string {
	return []string{"issue", "--caller-id", "7", "--label", "life", "--operator", "op", "--reason", "bootstrap"}
}

func signerAuthFullRotate() []string {
	return []string{"rotate", "--caller-id", "7", "--credential-id", "9", "--operator", "op", "--reason", "rotate"}
}

func signerAuthFullRevoke() []string {
	return []string{"revoke", "--credential-id", "9", "--operator", "op", "--reason", "retire"}
}

func signerAuthFullSetCanSign() []string {
	return []string{"set-can-sign", "--caller-id", "7", "--can-sign", "false", "--operator", "op", "--reason", "least-privilege"}
}

func TestSignerAuthUsageErrors(t *testing.T) {
	issue, rotate, revoke, flip := signerAuthFullIssue(), signerAuthFullRotate(), signerAuthFullRevoke(), signerAuthFullSetCanSign()
	cases := []struct {
		name string
		args []string
	}{
		{name: "no args", args: nil},
		{name: "unknown action", args: []string{"mint"}},
		{name: "issue missing caller-id", args: dropFlag(issue, "--caller-id")},
		{name: "issue missing label", args: dropFlag(issue, "--label")},
		{name: "issue missing operator", args: dropFlag(issue, "--operator")},
		{name: "issue missing reason", args: dropFlag(issue, "--reason")},
		{name: "issue zero caller-id", args: replaceFlag(issue, "--caller-id", "0")},
		{name: "issue positional", args: append(append([]string{}, issue...), "extra")},
		{name: "rotate missing credential-id", args: dropFlag(rotate, "--credential-id")},
		{name: "rotate zero credential-id", args: replaceFlag(rotate, "--credential-id", "0")},
		{name: "rotate positional", args: append(append([]string{}, rotate...), "extra")},
		{name: "revoke missing credential-id", args: dropFlag(revoke, "--credential-id")},
		{name: "revoke missing reason", args: dropFlag(revoke, "--reason")},
		{name: "revoke positional", args: append(append([]string{}, revoke...), "extra")},
		{name: "flip missing can-sign", args: dropFlag(flip, "--can-sign")},
		{name: "flip bad can-sign", args: replaceFlag(flip, "--can-sign", "maybe")},
		{name: "flip missing caller-id", args: dropFlag(flip, "--caller-id")},
		{name: "flip positional", args: append(append([]string{}, flip...), "extra")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := SignerAuth(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 2 {
				t.Fatalf("SignerAuth() exit code = %d, want 2; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: txharbor signer-auth") {
				t.Errorf("stderr %q lacks the usage line", stderr.String())
			}
		})
	}
}

func TestSignerAuthConfigError(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "issue", args: signerAuthFullIssue()},
		{name: "rotate", args: signerAuthFullRotate()},
		{name: "revoke", args: signerAuthFullRevoke()},
		{name: "set-can-sign", args: signerAuthFullSetCanSign()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := SignerAuth(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(map[string]string{}),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 1 {
				t.Fatalf("SignerAuth() exit code = %d, want 1; stderr=%s", code, stderr.String())
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

func TestSignerServeConfigError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := SignerServe(context.Background(), nil, Deps{
		Getenv: fakeEnv(map[string]string{}),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("SignerServe() exit code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "configuration error") {
		t.Errorf("stderr %q lacks the configuration error", stderr.String())
	}

	var stdout2, stderr2 bytes.Buffer
	code = SignerServe(context.Background(), []string{"extra"}, Deps{
		Getenv: fakeEnv(map[string]string{}),
		Stdout: &stdout2,
		Stderr: &stderr2,
	})
	if code != 2 {
		t.Fatalf("SignerServe(extra) exit code = %d, want 2", code)
	}
}
