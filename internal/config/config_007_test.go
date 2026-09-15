package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestLoadChainIDAndHTTPAddrPassthrough pins the T035 (007 US1) config
// passthrough contract: the withdrawal path consumes exactly two pre-existing
// fields — ChainID (the deployment chain bind for FR-04, from
// TXHARBOR_CHAIN_ID) and HTTPAddr (the reused listener address, from
// TXHARBOR_HTTP_ADDR) — and no 007-specific env var. Both already flow through
// config.Load, so this test is the acceptance evidence that the wiring needs
// no new carrier.
func TestLoadChainIDAndHTTPAddrPassthrough(t *testing.T) {
	env := baseEnv()
	env[EnvChainID] = "8453"
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ChainID != 8453 {
		t.Fatalf("ChainID = %d, want 8453 (TXHARBOR_CHAIN_ID deployment bind)", cfg.ChainID)
	}
	// baseEnv carries no TXHARBOR_HTTP_ADDR, so the listener default applies.
	if cfg.HTTPAddr != DefaultHTTPAddr {
		t.Fatalf("HTTPAddr = %q, want default %q", cfg.HTTPAddr, DefaultHTTPAddr)
	}

	// The same listener knob the 007 server reuses overrides the default.
	env[EnvHTTPAddr] = "0.0.0.0:9090"
	cfg, err = Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != "0.0.0.0:9090" {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, "0.0.0.0:9090")
	}
}

// TestLoadChainBindRefusesIllegalChainID mirrors the parseChainID refusal at
// the Load boundary (FR-04): every illegal TXHARBOR_CHAIN_ID aborts with a
// diagnostic naming the variable and returns no config, so the 007 wiring can
// never receive an unbound, zero, negative or non-numeric deployment chain.
func TestLoadChainBindRefusesIllegalChainID(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "missing", mutate: func(m map[string]string) { delete(m, EnvChainID) }},
		{name: "empty", mutate: func(m map[string]string) { m[EnvChainID] = "" }},
		{name: "non-integer", mutate: func(m map[string]string) { m[EnvChainID] = "abc" }},
		{name: "float", mutate: func(m map[string]string) { m[EnvChainID] = "1.5" }},
		{name: "zero", mutate: func(m map[string]string) { m[EnvChainID] = "0" }},
		{name: "negative", mutate: func(m map[string]string) { m[EnvChainID] = "-1" }},
		{name: "beyond uint64", mutate: func(m map[string]string) { m[EnvChainID] = "18446744073709551616" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			tc.mutate(env)
			cfg, err := Load(fakeEnv(env))
			if err == nil {
				t.Fatalf("Load() with %s TXHARBOR_CHAIN_ID: expected error", tc.name)
			}
			if cfg != nil {
				t.Fatalf("Load() returned config alongside error: %+v", cfg)
			}
			if !strings.Contains(err.Error(), EnvChainID) {
				t.Fatalf("error %q does not name %s", err, EnvChainID)
			}
		})
	}
}

// TestConfigSourceHasNo007SpecificEnvKnobs locks the plan wiring for 007:
// ChainID + HTTPAddr reuse only, no new secret knobs (T035). The check reads
// the package source so any future edit that smuggles a 007-specific variable
// into config.go fails loudly here.
func TestConfigSourceHasNo007SpecificEnvKnobs(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed to locate the config test file")
	}
	src, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "config.go"))
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	if strings.Contains(string(src), "TXHARBOR_WITHDRAWAL") {
		t.Fatal("config.go must not define 007-specific env knobs; 007 reuses TXHARBOR_CHAIN_ID + TXHARBOR_HTTP_ADDR")
	}
}
