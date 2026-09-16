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
	"log/slog"
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
