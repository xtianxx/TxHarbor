package app

import (
	"bytes"
	"context"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/xtianxx/txharbor/internal/config"
)

func fakeEnv(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func validEnv(addr string) map[string]string {
	return map[string]string{
		config.EnvPGDSN:    "postgres://txharbor:sup3rs3cret@127.0.0.1:5432/txharbor?sslmode=disable",
		config.EnvRPCURL:   "http://127.0.0.1:8545",
		config.EnvChainID:  "31337",
		config.EnvHTTPAddr: addr,
	}
}

// freePort reserves a local port and releases it so Serve can bind it later.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestServeRejectsBadConfigBeforeListening covers FR-001/002 (SC-002): every
// missing/invalid variable aborts startup with a diagnostic naming the
// variable, before any listener is opened.
func TestServeRejectsBadConfigBeforeListening(t *testing.T) {
	addr := freePort(t)

	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantVar string
	}{
		{name: "missing pg dsn", mutate: func(m map[string]string) { delete(m, config.EnvPGDSN) }, wantVar: config.EnvPGDSN},
		{name: "missing rpc url", mutate: func(m map[string]string) { delete(m, config.EnvRPCURL) }, wantVar: config.EnvRPCURL},
		{name: "missing chain id", mutate: func(m map[string]string) { delete(m, config.EnvChainID) }, wantVar: config.EnvChainID},
		{name: "invalid pg dsn", mutate: func(m map[string]string) { m[config.EnvPGDSN] = "not a dsn" }, wantVar: config.EnvPGDSN},
		{name: "invalid rpc url", mutate: func(m map[string]string) { m[config.EnvRPCURL] = "ftp://x" }, wantVar: config.EnvRPCURL},
		{name: "invalid chain id", mutate: func(m map[string]string) { m[config.EnvChainID] = "abc" }, wantVar: config.EnvChainID},
		{name: "invalid http addr", mutate: func(m map[string]string) { m[config.EnvHTTPAddr] = "nonsense" }, wantVar: config.EnvHTTPAddr},
		{name: "invalid startup timeout", mutate: func(m map[string]string) { m[config.EnvStartupTimeout] = "0s" }, wantVar: config.EnvStartupTimeout},
		{name: "invalid probe budget", mutate: func(m map[string]string) { m[config.EnvProbeInterval] = "6s" }, wantVar: config.EnvProbeInterval},
		{name: "invalid probe timeout", mutate: func(m map[string]string) { m[config.EnvProbeTimeout] = "0s" }, wantVar: config.EnvProbeTimeout},
		{name: "invalid shutdown timeout", mutate: func(m map[string]string) { m[config.EnvShutdownTimeout] = "abc" }, wantVar: config.EnvShutdownTimeout},
		{name: "invalid migrate lock timeout", mutate: func(m map[string]string) { m[config.EnvMigrateLockTimeout] = "-1s" }, wantVar: config.EnvMigrateLockTimeout},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnv(addr)
			tc.mutate(env)

			var stdout, stderr bytes.Buffer
			code := Serve(context.Background(), Deps{
				Getenv:  fakeEnv(env),
				Stdout:  &stdout,
				Stderr:  &stderr,
				Signals: make(chan os.Signal),
			})
			if code == 0 {
				t.Fatalf("Serve() exit code = 0, want non-zero; stderr=%s", stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantVar) {
				t.Fatalf("stderr %q does not name %s", stderr.String(), tc.wantVar)
			}
			if !strings.Contains(stderr.String(), "configuration error") {
				t.Fatalf("stderr %q is not a configuration error", stderr.String())
			}
			// No listener may be left behind on the configured address.
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("address %s still in use after config failure: %v; stderr=%s", addr, err, stderr.String())
			}
			ln.Close()
		})
	}
}

func TestServeMissingConfigListsEveryVariable(t *testing.T) {
	var stderr bytes.Buffer
	code := Serve(context.Background(), Deps{
		Getenv:  fakeEnv(map[string]string{}),
		Stderr:  &stderr,
		Signals: make(chan os.Signal),
	})
	if code == 0 {
		t.Fatal("Serve() exit code = 0, want non-zero")
	}
	for _, name := range []string{config.EnvPGDSN, config.EnvRPCURL, config.EnvChainID} {
		if !strings.Contains(stderr.String(), name) {
			t.Errorf("stderr %q does not name %s", stderr.String(), name)
		}
	}
}

// fullServeEnv is a fully valid Serve environment: every required variable is
// present so the confirmation-depth matrix below fails only on its own
// mutation (values mirror internal/config baseEnv).
func fullServeEnv(addr string) map[string]string {
	return map[string]string{
		config.EnvPGDSN:                 "postgres://txharbor:sup3rs3cret@127.0.0.1:5432/txharbor?sslmode=disable",
		config.EnvRPCURL:                "http://127.0.0.1:8545",
		config.EnvChainID:               "31337",
		config.EnvStartHeight:           "0",
		config.EnvLogStartHeight:        "0",
		config.EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		config.EnvDepositStartHeight:    "0",
		config.EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		config.EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		config.EnvConfirmationDepth:     "10",
		config.EnvReorgMaxDepth:         "100",
		config.EnvHTTPAddr:              addr,
	}
}

// TestServeRejectsIllegalConfirmationDepth pins the 005 FR-03/Q1 refusal at
// the process gate (T015, US2-illegal): every illegal
// TXHARBOR_CONFIRMATION_DEPTH aborts Serve with a diagnostic naming the
// variable, before any listener is opened.
func TestServeRejectsIllegalConfirmationDepth(t *testing.T) {
	addr := freePort(t)

	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantVar string
	}{
		{name: "missing", mutate: func(m map[string]string) { delete(m, config.EnvConfirmationDepth) }, wantVar: config.EnvConfirmationDepth},
		{name: "empty", mutate: func(m map[string]string) { m[config.EnvConfirmationDepth] = "" }, wantVar: config.EnvConfirmationDepth},
		{name: "non-integer", mutate: func(m map[string]string) { m[config.EnvConfirmationDepth] = "abc" }, wantVar: config.EnvConfirmationDepth},
		{name: "zero", mutate: func(m map[string]string) { m[config.EnvConfirmationDepth] = "0" }, wantVar: config.EnvConfirmationDepth},
		{name: "negative", mutate: func(m map[string]string) { m[config.EnvConfirmationDepth] = "-5" }, wantVar: config.EnvConfirmationDepth},
		{name: "beyond uint64", mutate: func(m map[string]string) { m[config.EnvConfirmationDepth] = "18446744073709551616" }, wantVar: config.EnvConfirmationDepth},
		{name: "beyond int64", mutate: func(m map[string]string) { m[config.EnvConfirmationDepth] = "9223372036854775808" }, wantVar: config.EnvConfirmationDepth},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := fullServeEnv(addr)
			tc.mutate(env)

			var stdout, stderr bytes.Buffer
			code := Serve(context.Background(), Deps{
				Getenv:  fakeEnv(env),
				Stdout:  &stdout,
				Stderr:  &stderr,
				Signals: make(chan os.Signal),
			})
			if code == 0 {
				t.Fatalf("Serve() exit code = 0, want non-zero; stderr=%s", stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantVar) {
				t.Fatalf("stderr %q does not name %s", stderr.String(), tc.wantVar)
			}
			if !strings.Contains(stderr.String(), "configuration error") {
				t.Fatalf("stderr %q is not a configuration error", stderr.String())
			}
			// No listener may be left behind on the configured address.
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("address %s still in use after config failure: %v; stderr=%s", addr, err, stderr.String())
			}
			ln.Close()
		})
	}
}

// TestServeRestartDifferentConfirmationDepthDiverges locks the US5-5
// drift-side precondition at the startup gate (T015): a restart carrying a
// different N freezes a different threshold into the effective configuration
// — no default, no normalization, no silent convergence. The refusal itself
// is enforced where the effective policy row is visible: the confirmation
// loop's verifyPolicyIdentity (T011) stops loud on mismatch (see
// TestConfirmationStartupDriftStops in internal/indexer/confirmscan_test.go:
// error type + zero commits + stopped state); the drift-exit/restart-recovery
// integration is T027, not here. Serve has no startup policy-row comparison
// of its own, so no new drift machinery is introduced by this test.
func TestServeRestartDifferentConfirmationDepthDiverges(t *testing.T) {
	addr := freePort(t)
	load := func(n string) uint64 {
		t.Helper()
		env := fullServeEnv(addr)
		env[config.EnvConfirmationDepth] = n
		cfg, err := config.Load(fakeEnv(env))
		if err != nil {
			t.Fatalf("Load() with confirmation depth %q: unexpected error %v", n, err)
		}
		return cfg.ConfirmationDepth
	}
	if got := load("10"); got != 10 {
		t.Fatalf("ConfirmationDepth = %d, want frozen 10", got)
	}
	if got := load("11"); got != 11 {
		t.Fatalf("ConfirmationDepth = %d, want frozen 11 (restart N must diverge, never converge)", got)
	}
}
