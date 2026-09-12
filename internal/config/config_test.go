package config

import (
	"strings"
	"testing"
	"time"
)

func fakeEnv(m map[string]string) Getenv {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func baseEnv() map[string]string {
	return map[string]string{
		EnvPGDSN:   "postgres://txharbor:sup3rs3cret@127.0.0.1:5432/txharbor?sslmode=disable",
		EnvRPCURL:  "http://127.0.0.1:8545",
		EnvChainID: "31337",
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != DefaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, DefaultHTTPAddr)
	}
	if cfg.StartupTimeout != DefaultStartupTimeout {
		t.Errorf("StartupTimeout = %s, want %s", cfg.StartupTimeout, DefaultStartupTimeout)
	}
	if cfg.ProbeInterval != DefaultProbeInterval {
		t.Errorf("ProbeInterval = %s, want %s", cfg.ProbeInterval, DefaultProbeInterval)
	}
	if cfg.ProbeTimeout != DefaultProbeTimeout {
		t.Errorf("ProbeTimeout = %s, want %s", cfg.ProbeTimeout, DefaultProbeTimeout)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}
	if cfg.MigrateLockTimeout != DefaultMigrateLockTimeout {
		t.Errorf("MigrateLockTimeout = %s, want %s", cfg.MigrateLockTimeout, DefaultMigrateLockTimeout)
	}
	if cfg.ChainID != 31337 {
		t.Errorf("ChainID = %d, want 31337", cfg.ChainID)
	}
}

func TestLoadCustomValues(t *testing.T) {
	env := baseEnv()
	env[EnvHTTPAddr] = "0.0.0.0:9090"
	env[EnvStartupTimeout] = "10s"
	env[EnvProbeInterval] = "1s"
	env[EnvProbeTimeout] = "4s"
	env[EnvShutdownTimeout] = "5s"
	env[EnvMigrateLockTimeout] = "45s"
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != "0.0.0.0:9090" || cfg.StartupTimeout != 10*time.Second ||
		cfg.ProbeInterval != time.Second || cfg.ProbeTimeout != 4*time.Second ||
		cfg.ShutdownTimeout != 5*time.Second || cfg.MigrateLockTimeout != 45*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadMissingRequiredNamesVariable(t *testing.T) {
	for _, name := range []string{EnvPGDSN, EnvRPCURL, EnvChainID} {
		t.Run(name, func(t *testing.T) {
			env := baseEnv()
			delete(env, name)
			_, err := Load(fakeEnv(env))
			if err == nil {
				t.Fatalf("Load() with missing %s: expected error", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Fatalf("error %q does not name %s", err, name)
			}
		})
	}
}

func TestLoadMissingAllReportsEveryVariable(t *testing.T) {
	_, err := Load(fakeEnv(map[string]string{}))
	if err == nil {
		t.Fatal("expected error for empty environment")
	}
	for _, name := range []string{EnvPGDSN, EnvRPCURL, EnvChainID} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
}

func TestLoadEmptyRequiredValueIsMissing(t *testing.T) {
	env := baseEnv()
	env[EnvChainID] = ""
	_, err := Load(fakeEnv(env))
	if err == nil || !strings.Contains(err.Error(), EnvChainID) {
		t.Fatalf("expected missing %s error, got %v", EnvChainID, err)
	}
}

func TestLoadInvalidValuesNameVariable(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		wantVar string
	}{
		{name: EnvPGDSN, value: "not a dsn", wantVar: EnvPGDSN},
		{name: EnvRPCURL, value: "ftp://127.0.0.1:8545", wantVar: EnvRPCURL},
		{name: EnvRPCURL, value: "not a url", wantVar: EnvRPCURL},
		{name: EnvChainID, value: "abc", wantVar: EnvChainID},
		{name: EnvChainID, value: "0", wantVar: EnvChainID},
		{name: EnvChainID, value: "-1", wantVar: EnvChainID},
		{name: EnvHTTPAddr, value: "nonsense", wantVar: EnvHTTPAddr},
		{name: EnvHTTPAddr, value: "127.0.0.1:99999", wantVar: EnvHTTPAddr},
		{name: EnvStartupTimeout, value: "0s", wantVar: EnvStartupTimeout},
		{name: EnvStartupTimeout, value: "abc", wantVar: EnvStartupTimeout},
		{name: EnvProbeInterval, value: "-1s", wantVar: EnvProbeInterval},
		{name: EnvProbeInterval, value: "11s", wantVar: EnvProbeInterval},
		{name: EnvProbeInterval, value: "6s", wantVar: EnvProbeInterval}, // 6s + 5s > 10s
		{name: EnvProbeTimeout, value: "0s", wantVar: EnvProbeTimeout},
		{name: EnvShutdownTimeout, value: "-1s", wantVar: EnvShutdownTimeout},
		{name: EnvMigrateLockTimeout, value: "abc", wantVar: EnvMigrateLockTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			env := baseEnv()
			env[tc.name] = tc.value
			_, err := Load(fakeEnv(env))
			if err == nil {
				t.Fatalf("Load() with %s=%q: expected error", tc.name, tc.value)
			}
			if !strings.Contains(err.Error(), tc.wantVar) {
				t.Fatalf("error %q does not name %s", err, tc.wantVar)
			}
		})
	}
}

func TestSummaryRedactsCredentials(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	summary := cfg.Summary()
	if strings.Contains(summary, "sup3rs3cret") {
		t.Fatalf("summary leaks credential: %s", summary)
	}
	if !strings.Contains(summary, "[REDACTED]") {
		t.Fatalf("summary missing redaction marker: %s", summary)
	}
	if !strings.Contains(summary, "127.0.0.1:5432") {
		t.Fatalf("summary lost non-sensitive context: %s", summary)
	}
}
