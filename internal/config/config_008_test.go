package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xtianxx/txharbor/internal/logx"
)

// TestLoadNonceReadTokenPassthrough pins the T016 passthrough contract: the
// token is an optional env knob that flows through Load verbatim, and an
// unset/empty value leaves the read endpoints fail-closed (empty token).
func TestLoadNonceReadTokenPassthrough(t *testing.T) {
	env := baseEnv()
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.NonceReadToken != "" {
		t.Fatalf("NonceReadToken = %q, want empty when %s is unset", cfg.NonceReadToken, EnvNonceReadToken)
	}

	const token = "008-read-token-abc123"
	env[EnvNonceReadToken] = token
	cfg, err = Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.NonceReadToken != token {
		t.Fatalf("NonceReadToken = %q, want %q", cfg.NonceReadToken, token)
	}

	// An explicitly empty value is treated as unset, never as a valid
	// credential.
	env[EnvNonceReadToken] = ""
	cfg, err = Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.NonceReadToken != "" {
		t.Fatalf("NonceReadToken = %q, want empty for an empty %s", cfg.NonceReadToken, EnvNonceReadToken)
	}
}

// TestNonceReadTokenNeverEchoedRaw pins credential hygiene: the raw token
// appears in neither Summary() (presence is rendered as the redaction
// placeholder) nor in a Load refusal that names another variable.
func TestNonceReadTokenNeverEchoedRaw(t *testing.T) {
	const token = "tok_live_008_SUPERSECRET"
	env := baseEnv()
	env[EnvNonceReadToken] = token
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	summary := cfg.Summary()
	if strings.Contains(summary, token) {
		t.Fatalf("Summary() leaks the raw nonce read token: %q", summary)
	}
	if !strings.Contains(summary, "nonce_read_token="+logx.Redacted) {
		t.Fatalf("Summary() %q does not render the token as %s", summary, logx.Redacted)
	}

	env[EnvChainID] = "not-a-chain"
	bad, err := Load(fakeEnv(env))
	if err == nil {
		t.Fatal("Load() with an illegal chain id: expected error")
	}
	if bad != nil {
		t.Fatalf("Load() returned config alongside error: %+v", bad)
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("Load() error leaks the raw nonce read token: %q", err)
	}
}

// TestConfig008ReusesExistingTimingKnobs pins the T016 timing contract:
// reconcile/observation timing reuses the INDEX knobs (no 008 timing knob),
// and config.go defines exactly one TXHARBOR_NONCE_* carrier.
func TestConfig008ReusesExistingTimingKnobs(t *testing.T) {
	env := baseEnv()
	env[EnvIndexRPCTimeout] = "7s"
	env[EnvIndexPollInterval] = "3s"
	env[EnvIndexRetryInitial] = "250ms"
	env[EnvIndexRetryMax] = "42s"
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.IndexRPCTimeout != 7*time.Second || cfg.IndexPollInterval != 3*time.Second ||
		cfg.IndexRetryInitial != 250*time.Millisecond || cfg.IndexRetryMax != 42*time.Second {
		t.Fatalf("INDEX timing knobs not carried: rpc=%s poll=%s initial=%s max=%s",
			cfg.IndexRPCTimeout, cfg.IndexPollInterval, cfg.IndexRetryInitial, cfg.IndexRetryMax)
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed to locate the config test file")
	}
	src, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "config.go"))
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	if n := strings.Count(string(src), "TXHARBOR_NONCE_"); n != 1 {
		t.Fatalf("config.go defines %d TXHARBOR_NONCE_* carriers, want exactly 1 (%s)", n, EnvNonceReadToken)
	}
	if EnvNonceReadToken != "TXHARBOR_NONCE_READ_TOKEN" {
		t.Fatalf("EnvNonceReadToken = %q, want TXHARBOR_NONCE_READ_TOKEN", EnvNonceReadToken)
	}
}
