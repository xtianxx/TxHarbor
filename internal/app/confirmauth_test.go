package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func confirmAuthArgs() []string {
	return []string{
		"--request-id", "op-1",
		"--expected-old-seq", "1",
		"--new-threshold", "25",
		"--operator", "op",
		"--reason", "raise depth",
	}
}

// TestConfirmAuthUsageErrors pins exit code 2 for every flag-shape failure:
// no args, each missing required flag, a non-integer old seq, an unknown
// flag, and trailing positionals. None of these reach config or the DB.
func TestConfirmAuthUsageErrors(t *testing.T) {
	full := confirmAuthArgs()
	cases := []struct {
		name string
		args []string
	}{
		{name: "no args", args: nil},
		{name: "missing request-id", args: dropFlag(full, "--request-id")},
		{name: "missing expected-old-seq", args: dropFlag(full, "--expected-old-seq")},
		{name: "missing new-threshold", args: dropFlag(full, "--new-threshold")},
		{name: "missing operator", args: dropFlag(full, "--operator")},
		{name: "missing reason", args: dropFlag(full, "--reason")},
		{name: "non-integer old seq", args: replaceFlag(full, "--expected-old-seq", "abc")},
		{name: "unknown flag", args: append(append([]string{}, full...), "--nope", "x")},
		{name: "positional", args: append(append([]string{}, full...), "extra")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := ConfirmAuth(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 2 {
				t.Fatalf("ConfirmAuth() exit code = %d, want 2; stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: txharbor confirm-auth") {
				t.Errorf("stderr %q lacks the usage line", stderr.String())
			}
		})
	}
}

// TestConfirmAuthConfigError pins exit code 1 (redacted) when the serve env
// is invalid: valid flags, empty env, so config.Load fails before any DB
// dial.
func TestConfirmAuthConfigError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := ConfirmAuth(context.Background(), confirmAuthArgs(), Deps{
		Getenv: fakeEnv(map[string]string{}),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if code != 1 {
		t.Fatalf("ConfirmAuth() exit code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "configuration error") {
		t.Errorf("stderr %q lacks the configuration error", stderr.String())
	}
	if !strings.Contains(stderr.String(), config.EnvPGDSN) {
		t.Errorf("stderr %q does not name %s", stderr.String(), config.EnvPGDSN)
	}
}

func dropFlag(args []string, name string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func replaceFlag(args []string, name, value string) []string {
	out := append([]string{}, args...)
	for i := 0; i+1 < len(out); i++ {
		if out[i] == name {
			out[i+1] = value
		}
	}
	return out
}
