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
				t.Fatalf("address %s still in use after config failure: %v", addr, err)
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
