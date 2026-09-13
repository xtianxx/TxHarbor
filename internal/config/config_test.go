package config

import (
	"crypto/sha256"
	"encoding/hex"
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
		EnvPGDSN:                 "postgres://txharbor:sup3rs3cret@127.0.0.1:5432/txharbor?sslmode=disable",
		EnvRPCURL:                "http://127.0.0.1:8545",
		EnvChainID:               "31337",
		EnvStartHeight:           "0",
		EnvLogStartHeight:        "0",
		EnvLogContracts:          "0x1111111111111111111111111111111111111111",
		EnvDepositStartHeight:    "0",
		EnvDepositContracts:      "0x1111111111111111111111111111111111111111",
		EnvDepositWatchAddresses: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
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
	for _, name := range []string{
		EnvPGDSN, EnvRPCURL, EnvChainID, EnvStartHeight, EnvLogStartHeight, EnvLogContracts,
		EnvDepositStartHeight, EnvDepositContracts, EnvDepositWatchAddresses,
	} {
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
	for _, name := range []string{
		EnvPGDSN, EnvRPCURL, EnvChainID, EnvStartHeight, EnvLogStartHeight, EnvLogContracts,
		EnvDepositStartHeight, EnvDepositContracts, EnvDepositWatchAddresses,
	} {
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

// --- 004 deposit detection (T002) ---

const (
	depositContractA = "0x1111111111111111111111111111111111111111"
	depositContractB = "0x2222222222222222222222222222222222222222"
	depositWatchA    = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	depositWatchB    = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	// Research R3 standard vector: the exact hashed preimage (printf with no
	// trailing newline) and its SHA-256.
	depositVectorInput = "deposit:v1\nstart:0\nasset:" + depositContractA + ":0\nwatch:" + depositWatchA + ":0"
	depositVectorHash  = "31822b65a6444c91bdaaa04a86582f4db25f35d5dd8ee02b7c2c12a4cf6600f0"
)

func TestLoadDepositDefaultsAndStandardVector(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DepositStartHeight != 0 {
		t.Errorf("DepositStartHeight = %d, want 0", cfg.DepositStartHeight)
	}
	if len(cfg.DepositContracts) != 1 ||
		cfg.DepositContracts[0] != (DepositEntry{Address: depositContractA, Effective: 0}) {
		t.Errorf("DepositContracts = %+v, want single entry with effective 0", cfg.DepositContracts)
	}
	if len(cfg.DepositWatchAddresses) != 1 ||
		cfg.DepositWatchAddresses[0] != (DepositEntry{Address: depositWatchA, Effective: 0}) {
		t.Errorf("DepositWatchAddresses = %+v, want single entry with effective 0", cfg.DepositWatchAddresses)
	}
	if cfg.DepositBatchBlocks != DefaultDepositBatchBlocks {
		t.Errorf("DepositBatchBlocks = %d, want default %d", cfg.DepositBatchBlocks, DefaultDepositBatchBlocks)
	}
	if cfg.DepositConfigHash != depositVectorHash {
		t.Errorf("DepositConfigHash = %q, want R3 vector %q", cfg.DepositConfigHash, depositVectorHash)
	}
}

// TestDepositIdentityStandardVector pins the exact bytes of the identity
// preimage (domain separator, no BOM, no trailing newline) and re-derives the
// R3 digest independently of the production code path.
func TestDepositIdentityStandardVector(t *testing.T) {
	contracts := []DepositEntry{{Address: depositContractA, Effective: 0}}
	watches := []DepositEntry{{Address: depositWatchA, Effective: 0}}
	lines := []string{"asset:" + depositContractA + ":0", "watch:" + depositWatchA + ":0"}

	if got := depositIdentityInput(0, lines); got != depositVectorInput {
		t.Fatalf("identity input = %q, want %q", got, depositVectorInput)
	}
	if strings.HasPrefix(depositVectorInput, "\ufeff") || strings.HasSuffix(depositVectorInput, "\n") {
		t.Fatal("vector preimage must carry no BOM and no trailing newline")
	}
	if got := depositIdentity(0, contracts, watches); got != depositVectorHash {
		t.Fatalf("depositIdentity = %q, want %q", got, depositVectorHash)
	}
	sum := sha256.Sum256([]byte(depositVectorInput))
	if hex.EncodeToString(sum[:]) != depositVectorHash {
		t.Fatalf("independent digest of pinned preimage = %q, want %q",
			hex.EncodeToString(sum[:]), depositVectorHash)
	}
}

func TestParseDepositEntriesNormalizes(t *testing.T) {
	entries, err := parseDepositEntries(
		"0x2222222222222222222222222222222222222222,0X1111111111111111111111111111111111111111", 0)
	if err != nil {
		t.Fatalf("parseDepositEntries() error = %v", err)
	}
	want := []DepositEntry{
		{Address: depositContractA, Effective: 0},
		{Address: depositContractB, Effective: 0},
	}
	assertEntries(t, entries, want)

	// Checksummed mixed case and a bare 40-hex address both converge to the
	// lowercase 0x-prefixed canonical form (FR-04).
	entries, err = parseDepositEntries("0xAbCdEf1234567890abcDEF1234567890ABCdEF12,abcdef1234567890abcdef1234567890abcdef12", 0)
	if err != nil {
		t.Fatalf("parseDepositEntries() error = %v", err)
	}
	want = []DepositEntry{{Address: "0xabcdef1234567890abcdef1234567890abcdef12", Effective: 0}}
	assertEntries(t, entries, want)

	// Exact duplicates (including case variants) collapse; the same address at
	// different heights stays distinct and sorts by canonical line.
	entries, err = parseDepositEntries(
		"0x1111111111111111111111111111111111111111:5,0x1111111111111111111111111111111111111111:10,"+
			"0x1111111111111111111111111111111111111111:10,0x1111111111111111111111111111111111111111", 0)
	if err != nil {
		t.Fatalf("parseDepositEntries() error = %v", err)
	}
	want = []DepositEntry{
		{Address: depositContractA, Effective: 0},
		{Address: depositContractA, Effective: 10},
		{Address: depositContractA, Effective: 5},
	}
	assertEntries(t, entries, want)

	// Without an explicit suffix the effective height defaults to the global
	// start; an explicit `:height` overrides it, including height 0.
	entries, err = parseDepositEntries("0x1111111111111111111111111111111111111111,0x2222222222222222222222222222222222222222:0", 100)
	if err != nil {
		t.Fatalf("parseDepositEntries() error = %v", err)
	}
	want = []DepositEntry{
		{Address: depositContractA, Effective: 100},
		{Address: depositContractB, Effective: 0},
	}
	assertEntries(t, entries, want)
}

func TestParseDepositEntriesRejects(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "empty", raw: ""},
		{name: "blank", raw: "   "},
		{name: "blank entry", raw: "0x1111111111111111111111111111111111111111,,0x2222222222222222222222222222222222222222"},
		{name: "short address", raw: "0x1234"},
		{name: "not hex", raw: "not-an-address"},
		{name: "empty effective", raw: "0x1111111111111111111111111111111111111111:"},
		{name: "negative effective", raw: "0x1111111111111111111111111111111111111111:-1"},
		{name: "float effective", raw: "0x1111111111111111111111111111111111111111:1.5"},
		{name: "non-decimal effective", raw: "0x1111111111111111111111111111111111111111:0x10"},
		{name: "extra colon", raw: "0x1111111111111111111111111111111111111111:5:6"},
		{name: "effective overflow", raw: "0x1111111111111111111111111111111111111111:18446744073709551616"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseDepositEntries(tc.raw, 0); err == nil {
				t.Fatalf("parseDepositEntries(%q): expected error", tc.raw)
			}
		})
	}
}

func TestLoadRejectsInvalidDepositConfiguration(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantVar string
	}{
		{name: "missing start", mutate: func(m map[string]string) { delete(m, EnvDepositStartHeight) }, wantVar: EnvDepositStartHeight},
		{name: "blank start", mutate: func(m map[string]string) { m[EnvDepositStartHeight] = "" }, wantVar: EnvDepositStartHeight},
		{name: "negative start", mutate: func(m map[string]string) { m[EnvDepositStartHeight] = "-1" }, wantVar: EnvDepositStartHeight},
		{name: "non-decimal start", mutate: func(m map[string]string) { m[EnvDepositStartHeight] = "1.5" }, wantVar: EnvDepositStartHeight},
		{name: "missing contracts", mutate: func(m map[string]string) { delete(m, EnvDepositContracts) }, wantVar: EnvDepositContracts},
		{name: "blank contracts", mutate: func(m map[string]string) { m[EnvDepositContracts] = "   " }, wantVar: EnvDepositContracts},
		{name: "empty contracts list", mutate: func(m map[string]string) { m[EnvDepositContracts] = "," }, wantVar: EnvDepositContracts},
		{name: "invalid contract", mutate: func(m map[string]string) { m[EnvDepositContracts] = "0x1234" }, wantVar: EnvDepositContracts},
		{name: "missing watches", mutate: func(m map[string]string) { delete(m, EnvDepositWatchAddresses) }, wantVar: EnvDepositWatchAddresses},
		{name: "blank watches", mutate: func(m map[string]string) { m[EnvDepositWatchAddresses] = " " }, wantVar: EnvDepositWatchAddresses},
		{name: "invalid watch effective", mutate: func(m map[string]string) { m[EnvDepositWatchAddresses] = depositWatchA + ":abc" }, wantVar: EnvDepositWatchAddresses},
		{name: "zero batch blocks", mutate: func(m map[string]string) { m[EnvDepositBatchBlocks] = "0" }, wantVar: EnvDepositBatchBlocks},
		{name: "negative batch blocks", mutate: func(m map[string]string) { m[EnvDepositBatchBlocks] = "-5" }, wantVar: EnvDepositBatchBlocks},
		{name: "non-decimal batch blocks", mutate: func(m map[string]string) { m[EnvDepositBatchBlocks] = "abc" }, wantVar: EnvDepositBatchBlocks},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			tc.mutate(env)
			cfg, err := Load(fakeEnv(env))
			if err == nil {
				t.Fatalf("Load() with %s: expected error", tc.name)
			}
			if cfg != nil {
				t.Fatalf("Load() returned config alongside error: %+v", cfg)
			}
			if !strings.Contains(err.Error(), tc.wantVar) {
				t.Fatalf("error %q does not name %s", err, tc.wantVar)
			}
		})
	}
}

func TestLoadDepositIdentityConvergence(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	baseHash := cfg.DepositConfigHash

	variants := []struct {
		name      string
		contracts string
		watches   string
		start     string
	}{
		{
			name:      "order duplicates spaces and case",
			contracts: "0x1111111111111111111111111111111111111111, 0x1111111111111111111111111111111111111111",
			watches:   "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			start:     "0",
		},
		{
			name:      "0X prefix",
			contracts: "0X1111111111111111111111111111111111111111",
			watches:   "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			start:     "0",
		},
		{
			name:      "explicit height equal to default start",
			contracts: depositContractA + ":0",
			watches:   depositWatchA,
			start:     "0",
		},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			env := baseEnv()
			env[EnvDepositStartHeight] = v.start
			env[EnvDepositContracts] = v.contracts
			env[EnvDepositWatchAddresses] = v.watches
			cfg, err := Load(fakeEnv(env))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.DepositConfigHash != baseHash {
				t.Fatalf("DepositConfigHash = %q, want converged %q", cfg.DepositConfigHash, baseHash)
			}
		})
	}
}

func TestLoadDepositIdentityChangesWithSemantics(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	baseHash := cfg.DepositConfigHash
	cases := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{name: "add contract", mutate: func(m map[string]string) {
			m[EnvDepositContracts] = depositContractA + "," + depositContractB
		}},
		{name: "add watch address", mutate: func(m map[string]string) {
			m[EnvDepositWatchAddresses] = depositWatchA + "," + depositWatchB
		}},
		{name: "change effective height", mutate: func(m map[string]string) {
			m[EnvDepositContracts] = depositContractA + ":5"
		}},
		{name: "change start height", mutate: func(m map[string]string) {
			m[EnvDepositStartHeight] = "1"
		}},
		{name: "move entry between collections", mutate: func(m map[string]string) {
			m[EnvDepositContracts] = depositWatchA
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			tc.mutate(env)
			cfg, err := Load(fakeEnv(env))
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.DepositConfigHash == baseHash {
				t.Fatalf("DepositConfigHash unchanged (%q) after %s", baseHash, tc.name)
			}
		})
	}
}

// TestLoadDepositIdentityExcludesTuningKnobs locks FR-06: batch and timeout
// parameters never enter the identity.
func TestLoadDepositIdentityExcludesTuningKnobs(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	baseHash := cfg.DepositConfigHash
	env := baseEnv()
	env[EnvDepositBatchBlocks] = "17"
	env[EnvIndexPollInterval] = "9s"
	env[EnvIndexRetryInitial] = "1s"
	env[EnvIndexRetryMax] = "2m"
	cfg, err = Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DepositConfigHash != baseHash {
		t.Fatalf("DepositConfigHash = %q, want identity independent of tuning knobs %q",
			cfg.DepositConfigHash, baseHash)
	}
}

func TestLoadDepositEffectiveDefaultsToGlobalStart(t *testing.T) {
	env := baseEnv()
	env[EnvDepositStartHeight] = "100"
	env[EnvDepositContracts] = depositContractB + "," + depositContractA + ":5"
	env[EnvDepositWatchAddresses] = depositWatchA
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	assertEntries(t, cfg.DepositContracts, []DepositEntry{
		{Address: depositContractA, Effective: 5},
		{Address: depositContractB, Effective: 100},
	})
	assertEntries(t, cfg.DepositWatchAddresses, []DepositEntry{
		{Address: depositWatchA, Effective: 100},
	})
}

func TestDepositSnapshotEncoding(t *testing.T) {
	entries := []DepositEntry{
		{Address: depositContractB, Effective: 3},
		{Address: depositContractA, Effective: 10},
		{Address: depositContractA, Effective: 5},
	}
	got := DepositSnapshot(entries)
	want := depositContractA + ":10\n" + depositContractA + ":5\n" + depositContractB + ":3"
	if got != want {
		t.Fatalf("DepositSnapshot = %q, want %q", got, want)
	}
	if strings.HasSuffix(got, "\n") {
		t.Fatalf("DepositSnapshot has trailing newline: %q", got)
	}
	if strings.Contains(got, "asset:") || strings.Contains(got, "watch:") {
		t.Fatalf("history snapshot lines must be <address>:<effective> without stream prefix: %q", got)
	}
	if got := DepositSnapshot(nil); got != "" {
		t.Fatalf("DepositSnapshot(nil) = %q, want empty", got)
	}

	// The Load path feeds the same encoder T025 will use for history rows.
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := DepositSnapshot(cfg.DepositContracts); got != depositContractA+":0" {
		t.Fatalf("contracts snapshot = %q, want %q", got, depositContractA+":0")
	}
	if got := DepositSnapshot(cfg.DepositWatchAddresses); got != depositWatchA+":0" {
		t.Fatalf("watches snapshot = %q, want %q", got, depositWatchA+":0")
	}
}

// TestSummaryDepositFieldsRedacted locks the FR-15 startup echo: deposit
// state is visible as counts and identity, never as a raw address dump.
func TestSummaryDepositFieldsRedacted(t *testing.T) {
	env := baseEnv()
	env[EnvDepositContracts] = depositContractA + "," + depositContractB
	cfg, err := Load(fakeEnv(env))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	summary := cfg.Summary()
	for _, want := range []string{
		"deposit_start_height=0",
		"deposit_contracts=2",
		"deposit_watch_addresses=1",
		"deposit_config_hash=" + cfg.DepositConfigHash,
		"deposit_batch_blocks=500",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary %q does not contain %q", summary, want)
		}
	}
	for _, leak := range []string{depositContractA, depositContractB, depositWatchA, "sup3rs3cret"} {
		if strings.Contains(summary, leak) {
			t.Errorf("summary leaks raw value %q: %s", leak, summary)
		}
	}
}

func assertEntries(t *testing.T, got, want []DepositEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("entries = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries[%d] = %+v, want %+v (all: %+v)", i, got[i], want[i], got)
		}
	}
}
