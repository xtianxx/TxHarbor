// withdrawalauthz_secrecy_test.go is the T014 credential-secrecy half that runs
// without a database: the API key presented on the supply path must never reach
// CLI stdout/stderr or any slog record, and the carrier must accept the
// `--api-key` flag (exit 1 from config/pool/auth, never a 2 flag-usage error).
//
// RED until T013 wires `--api-key` + Authenticate into the supply carrier:
// today the flag is unknown, so flag.Parse returns 2 and no permission predicate
// or secret-handling path exists. The secrecy assertions still hold trivially on
// the usage-error path; the acceptance assertion is the recorded T013 gap.
package app

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/withdrawal"
)

// TestWithdrawalAuthzSupplyKeySecrecyWithoutDB covers both a well-formed
// presented key (would resolve through Authenticate once T013 lands) and a
// malformed one (shape-rejected pre-pool). In neither case may the secret
// value appear on stdout, on stderr, or in a captured slog record.
func TestWithdrawalAuthzSupplyKeySecrecyWithoutDB(t *testing.T) {
	wellFormed, _, _, err := withdrawal.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cases := []struct {
		name string
		key  string
	}{
		{name: "well-formed key", key: wellFormed},
		{name: "malformed key", key: "txh_not-a-real-presented-key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origLog := slog.Default()
			var slogBuf bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&slogBuf, nil)))
			defer slog.SetDefault(origLog)

			args := append(withdrawalAuthzSupplyArgs(), "--api-key", tc.key)
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), args, Deps{
				Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
				Stdout: &stdout,
				Stderr: &stderr,
			})

			// T013 gap (recorded red): the secret flag is not yet a supply flag.
			if code == 2 || strings.Contains(stderr.String(), "flag provided but not defined") {
				t.Fatalf("supply carrier does not accept --api-key yet (exit %d); stderr=%s", code, stderr.String())
			}
			if code != 1 {
				t.Fatalf("supply exit code = %d, want 1 (config/pool/auth failure, never 2); stderr=%s", code, stderr.String())
			}

			for _, out := range []struct{ name, text string }{
				{"stdout", stdout.String()},
				{"stderr", stderr.String()},
				{"slog capture", slogBuf.String()},
			} {
				if strings.Contains(out.text, tc.key) {
					t.Fatalf("presented API key leaked into %s: %q", out.name, out.text)
				}
			}
		})
	}
}

// writeKeyFile writes one credential to a 0600 temp file and returns its path.
// content is written verbatim so tests can exercise trimming and empty files.
func writeKeyFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "api-key")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	return path
}

// TestWithdrawalAuthzSupplyKeyFileSecrecyWithoutDB is the file-input half of the
// secrecy proof: a key read from --api-key-file (with a trailing newline, which
// must be trimmed away) is consumed exactly like --api-key, exits 1 on the
// config/pool/auth failure path rather than 2, and never appears on stdout, on
// stderr, or in a captured slog record. A malformed file-held key is refused on
// the same exit-1 path, never as a flag-usage error.
func TestWithdrawalAuthzSupplyKeyFileSecrecyWithoutDB(t *testing.T) {
	wellFormed, _, _, err := withdrawal.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cases := []struct {
		name    string
		content string
	}{
		{name: "well-formed key with trailing newline", content: wellFormed + "\n"},
		{name: "well-formed key with surrounding whitespace", content: "  " + wellFormed + "  \n"},
		{name: "malformed key", content: "txh_not-a-real-presented-key\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origLog := slog.Default()
			var slogBuf bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&slogBuf, nil)))
			defer slog.SetDefault(origLog)

			path := writeKeyFile(t, tc.content)
			args := append(withdrawalAuthzSupplyArgs(), "--api-key-file", path)
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), args, Deps{
				Getenv: fakeEnv(fullServeEnv("127.0.0.1:0")),
				Stdout: &stdout,
				Stderr: &stderr,
			})

			if code == 2 || strings.Contains(stderr.String(), "flag provided but not defined") {
				t.Fatalf("supply carrier does not accept --api-key-file (exit %d); stderr=%s", code, stderr.String())
			}
			if code != 1 {
				t.Fatalf("supply exit code = %d, want 1 (config/pool/auth failure, never 2); stderr=%s", code, stderr.String())
			}

			secret := strings.TrimSpace(tc.content)
			for _, out := range []struct{ name, text string }{
				{"stdout", stdout.String()},
				{"stderr", stderr.String()},
				{"slog capture", slogBuf.String()},
			} {
				if strings.Contains(out.text, secret) {
					t.Fatalf("file-held API key leaked into %s: %q", out.name, out.text)
				}
			}
		})
	}
}

// TestWithdrawalAuthzSupplyKeyInputFailureModes pins the deterministic exit-2
// failures of --api-key-file: a missing path, a path that cannot be read as a
// file (a directory), an empty value, and a file that is empty or whitespace
// only. Each is a flag-input error detected before config.Load, proven by an
// empty environment still returning 2 (config failure would be 1) and by no
// credential content reaching any output channel.
func TestWithdrawalAuthzSupplyKeyInputFailureModes(t *testing.T) {
	dirPath := t.TempDir()
	cases := []struct {
		name string
		path string
	}{
		{name: "missing file", path: filepath.Join(t.TempDir(), "absent")},
		{name: "unreadable path", path: dirPath},
		{name: "empty flag value", path: ""},
		{name: "empty file", path: writeKeyFile(t, "")},
		{name: "whitespace-only file", path: writeKeyFile(t, "  \n\t\n")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append(withdrawalAuthzSupplyArgs(), "--api-key-file", tc.path)
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), args, Deps{
				Getenv: fakeEnv(map[string]string{}),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 2 {
				t.Fatalf("exit code = %d, want 2 (flag-input error before config); stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "--api-key-file") {
				t.Fatalf("stderr %q does not name --api-key-file", stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: txharbor withdrawal-authz") {
				t.Errorf("stderr %q lacks the usage line", stderr.String())
			}
			if strings.Contains(stderr.String(), "configuration error") {
				t.Errorf("stderr %q reached config.Load before rejecting the file", stderr.String())
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}

// TestWithdrawalAuthzSupplyKeyInputConflict pins the no-precedence rule: passing
// both --api-key and --api-key-file (even an explicitly empty --api-key) is a
// usage error with no secret in the output, not a silent winner pick.
func TestWithdrawalAuthzSupplyKeyInputConflict(t *testing.T) {
	wellFormed, _, _, err := withdrawal.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	path := writeKeyFile(t, wellFormed+"\n")
	cases := []struct {
		name string
		args []string
	}{
		{name: "both non-empty", args: append(withdrawalAuthzSupplyArgs(), "--api-key", wellFormed, "--api-key-file", path)},
		{name: "explicit empty api-key plus file", args: append(withdrawalAuthzSupplyArgs(), "--api-key", "", "--api-key-file", path)},
		{name: "file plus api-key", args: append(withdrawalAuthzSupplyArgs(), "--api-key-file", path, "--api-key", wellFormed)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := WithdrawalAuthz(context.Background(), tc.args, Deps{
				Getenv: fakeEnv(map[string]string{}),
				Stdout: &stdout,
				Stderr: &stderr,
			})
			if code != 2 {
				t.Fatalf("exit code = %d, want 2 (mutually exclusive usage error); stderr=%s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "mutually exclusive") {
				t.Fatalf("stderr %q does not report the conflict", stderr.String())
			}
			if strings.Contains(stderr.String(), "configuration error") {
				t.Errorf("stderr %q reached config.Load before rejecting the conflict", stderr.String())
			}
			for _, text := range []string{stdout.String(), stderr.String()} {
				if strings.Contains(text, wellFormed) {
					t.Fatalf("API key leaked into conflict output: %q", text)
				}
			}
		})
	}
}

// TestWithdrawalAuthzUsagePresentsFileInputFirst pins the help-text contract:
// the usage document presents --api-key-file and marks --api-key as discouraged,
// so the discouraged plaintext form is never advertised as the default.
func TestWithdrawalAuthzUsagePresentsFileInputFirst(t *testing.T) {
	var stderr bytes.Buffer
	code := WithdrawalAuthz(context.Background(), []string{"supply", "--help"}, Deps{
		Getenv: fakeEnv(map[string]string{}),
		Stdout: io.Discard,
		Stderr: &stderr,
	})
	if code != 2 {
		t.Fatalf("--help exit code = %d, want 2 (usage); stderr=%s", code, stderr.String())
	}
	text := stderr.String()
	if !strings.Contains(text, "--api-key-file PATH") {
		t.Errorf("usage %q does not present --api-key-file", text)
	}
	if !strings.Contains(text, "discouraged") {
		t.Errorf("usage %q does not mark --api-key discouraged", text)
	}
	if strings.Index(text, "--api-key-file") > strings.Index(text, "--api-key KEY") {
		t.Errorf("usage %q presents the plaintext --api-key form before --api-key-file", text)
	}
}
