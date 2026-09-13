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
		EnvPGDSN:          "postgres://txharbor:sup3rs3cret@127.0.0.1:5432/txharbor?sslmode=disable",
		EnvRPCURL:         "http://127.0.0.1:8545",
		EnvChainID:        "31337",
		EnvStartHeight:    "0",
		EnvLogStartHeight: "0",
		EnvLogContracts:   "0x1111111111111111111111111111111111111111",
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
	if cfg.IndexRPCTimeout != DefaultIndexRPCTimeout {
		t.Errorf("IndexRPCTimeout = %s, want %s", cfg.IndexRPCTimeout, DefaultIndexRPCTimeout)
	}
	if cfg.IndexPollInterval != DefaultIndexPollInterval {
		t.Errorf("IndexPollInterval = %s, want %s", cfg.IndexPollInterval, DefaultIndexPollInterval)
	}
	if cfg.IndexRetryInitial != DefaultIndexRetryInitial {
		t.Errorf("IndexRetryInitial = %s, want %s", cfg.IndexRetryInitial, DefaultIndexRetryInitial)
	}
	if cfg.IndexRetryMax != DefaultIndexRetryMax {
		t.Errorf("IndexRetryMax = %s, want %s", cfg.IndexRetryMax, DefaultIndexRetryMax)
	}
	if cfg.ChainID != 31337 {
		t.Errorf("ChainID = %d, want 31337", cfg.ChainID)
	}
	if cfg.StartHeight != 0 {
		t.Errorf("StartHeight = %d, want 0 (genesis is legal)", cfg.StartHeight)
	}
	if cfg.LogStartHeight != 0 {
		t.Errorf("LogStartHeight = %d, want 0", cfg.LogStartHeight)
	}
	if len(cfg.LogContracts) != 1 || cfg.LogContracts[0] != "0x1111111111111111111111111111111111111111" {
		t.Errorf("LogContracts = %v, want single normalized address", cfg.LogContracts)
	}
	if len(cfg.LogConfigHash) != 64 {
		t.Errorf("LogConfigHash = %q, want 64 lowercase hex", cfg.LogConfigHash)
	}
	if cfg.LogBatchBlocks != DefaultLogBatchBlocks {
		t.Errorf("LogBatchBlocks = %d, want default %d", cfg.LogBatchBlocks, DefaultLogBatchBlocks)
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
	env[EnvStartHeight] = "12345"
	env[EnvIndexRPCTimeout] = "3s"
	env[EnvIndexPollInterval] = "500ms"
	env[EnvIndexRetryInitial] = "100ms"
	env[EnvIndexRetryMax] = "1m"
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != "0.0.0.0:9090" || cfg.StartupTimeout != 10*time.Second ||
		cfg.ProbeInterval != time.Second || cfg.ProbeTimeout != 4*time.Second ||
		cfg.ShutdownTimeout != 5*time.Second || cfg.MigrateLockTimeout != 45*time.Second ||
		cfg.StartHeight != 12345 || cfg.IndexRPCTimeout != 3*time.Second ||
		cfg.IndexPollInterval != 500*time.Millisecond ||
		cfg.IndexRetryInitial != 100*time.Millisecond || cfg.IndexRetryMax != time.Minute {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadMissingRequiredNamesVariable(t *testing.T) {
	for _, name := range []string{EnvPGDSN, EnvRPCURL, EnvChainID, EnvStartHeight, EnvLogStartHeight, EnvLogContracts} {
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
	for _, name := range []string{EnvPGDSN, EnvRPCURL, EnvChainID, EnvStartHeight, EnvLogStartHeight, EnvLogContracts} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
}

func TestLoadEmptyRequiredValueIsMissing(t *testing.T) {
	for _, name := range []string{EnvChainID, EnvStartHeight} {
		t.Run(name, func(t *testing.T) {
			env := baseEnv()
			env[name] = ""
			_, err := Load(fakeEnv(env))
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("expected missing %s error, got %v", name, err)
			}
		})
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
		{name: EnvStartHeight, value: "abc", wantVar: EnvStartHeight},
		{name: EnvStartHeight, value: "-1", wantVar: EnvStartHeight},
		{name: EnvStartHeight, value: "1.5", wantVar: EnvStartHeight},
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
		{name: EnvIndexRPCTimeout, value: "0s", wantVar: EnvIndexRPCTimeout},
		{name: EnvIndexRPCTimeout, value: "abc", wantVar: EnvIndexRPCTimeout},
		{name: EnvIndexPollInterval, value: "-1s", wantVar: EnvIndexPollInterval},
		{name: EnvIndexRetryInitial, value: "abc", wantVar: EnvIndexRetryInitial},
		{name: EnvIndexRetryMax, value: "0s", wantVar: EnvIndexRetryMax},
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
	env := baseEnv()
	// URL-embedded userinfo and query token must both be redacted (FR-15).
	env[EnvRPCURL] = "http://alice:rpcS3cret@rpc.example.com/v1?token=tok3nValue"
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	summary := cfg.Summary()
	for _, secret := range []string{"sup3rs3cret", "rpcS3cret", "tok3nValue"} {
		if strings.Contains(summary, secret) {
			t.Fatalf("summary leaks credential %q: %s", secret, summary)
		}
	}
	if !strings.Contains(summary, "[REDACTED]") {
		t.Fatalf("summary missing redaction marker: %s", summary)
	}
	if !strings.Contains(summary, "127.0.0.1:5432") {
		t.Fatalf("summary lost non-sensitive context: %s", summary)
	}
	if !strings.Contains(summary, "rpc.example.com") {
		t.Fatalf("summary lost non-sensitive RPC context: %s", summary)
	}
}

func TestNormalizeWhitelistStandardVector(t *testing.T) {
	contracts, hash, err := NormalizeWhitelist(
		"0x1111111111111111111111111111111111111111,0x2222222222222222222222222222222222222222")
	if err != nil {
		t.Fatalf("NormalizeWhitelist() error = %v", err)
	}
	want := []string{
		"0x1111111111111111111111111111111111111111",
		"0x2222222222222222222222222222222222222222",
	}
	if len(contracts) != len(want) {
		t.Fatalf("contracts = %v, want %v", contracts, want)
	}
	for i := range want {
		if contracts[i] != want[i] {
			t.Fatalf("contracts = %v, want %v", contracts, want)
		}
	}
	// Research R9 V2 vector (sha256sum-verified, no trailing newline).
	const wantHash = "b4eeddb97cb6ab1ba66eb8bb97e43b3f11b466de859f10bf30497ebd2e9cef7f"
	if hash != wantHash {
		t.Fatalf("hash = %q, want %q", hash, wantHash)
	}
}

func TestNormalizeWhitelistConvergesCaseOrderDupes(t *testing.T) {
	base := "0x1111111111111111111111111111111111111111,0x2222222222222222222222222222222222222222"
	_, baseHash, err := NormalizeWhitelist(base)
	if err != nil {
		t.Fatalf("NormalizeWhitelist() error = %v", err)
	}
	variants := []string{
		"0x2222222222222222222222222222222222222222,0x1111111111111111111111111111111111111111",
		"0x1111111111111111111111111111111111111111, 0x2222222222222222222222222222222222222222 ,0x1111111111111111111111111111111111111111",
		"0xABcDEF1234567890abcDEF1234567890ABCdEF12,0x1111111111111111111111111111111111111111,0x2222222222222222222222222222222222222222,0xabcdef1234567890abcdef1234567890abcdef12",
	}
	// First two variants must converge to the base identity; the third adds an
	// address and must differ.
	for i, v := range variants {
		contracts, hash, err := NormalizeWhitelist(v)
		if err != nil {
			t.Fatalf("variant %d: error = %v", i, err)
		}
		if i < 2 {
			if hash != baseHash {
				t.Fatalf("variant %d: hash = %q, want base %q", i, hash, baseHash)
			}
		} else {
			if hash == baseHash {
				t.Fatalf("variant %d: added address must change identity", i)
			}
			if len(contracts) != 3 || contracts[2] != "0xabcdef1234567890abcdef1234567890abcdef12" {
				t.Fatalf("variant %d: contracts = %v", i, contracts)
			}
		}
	}
}

func TestNormalizeWhitelistRejects(t *testing.T) {
	for _, raw := range []string{
		"",
		"   ",
		"0x1111111111111111111111111111111111111111,,0x2222222222222222222222222222222222222222",
		"not-an-address",
		"0x1234",
	} {
		if _, _, err := NormalizeWhitelist(raw); err == nil {
			t.Fatalf("NormalizeWhitelist(%q): expected error", raw)
		}
	}
}

func TestLoadLogBatchBlocks(t *testing.T) {
	env := baseEnv()
	env[EnvLogBatchBlocks] = "100"
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.LogBatchBlocks != 100 {
		t.Fatalf("LogBatchBlocks = %d, want 100", cfg.LogBatchBlocks)
	}
	for _, raw := range []string{"0", "-5", "abc"} {
		env[EnvLogBatchBlocks] = raw
		if _, err := Load(fakeEnv(env)); err == nil {
			t.Fatalf("Load() with batch %q: expected error", raw)
		}
	}
}
