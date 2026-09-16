package config

import (
	"strings"
	"testing"
	"time"
)

func signerEnv() map[string]string {
	env := baseEnv()
	env[EnvSignerHTTPAddr] = "127.0.0.1:8091"
	env[EnvSignerMode] = "development"
	env[EnvSignerKeyFile] = "/run/signer/test.key"
	env[EnvSignerKeyTimeout] = "5s"
	env[EnvSignerChains] = "31337"
	env[EnvSignerSenders] = "0x1111111111111111111111111111111111111111"
	env[EnvSignerAssets] = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	env[EnvSignerRecipients] = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	env[EnvSignerMaxAmount] = "2000000"
	env[EnvSignerMaxGasLimit] = "100000"
	env[EnvSignerMaxFeePerGas] = "2000000000"
	env[EnvSignerMaxPriorityFee] = "1500000000"
	env[EnvSignerMaxGasPrice] = "2000000000"
	return env
}

func TestSignerDefaults(t *testing.T) {
	cfg, err := Load(fakeEnv(baseEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.SignerHTTPAddr != "127.0.0.1:8091" {
		t.Fatalf("SignerHTTPAddr = %q", cfg.SignerHTTPAddr)
	}
	if cfg.SignerMode != "production" {
		t.Fatalf("SignerMode = %q, want fail-closed production", cfg.SignerMode)
	}
	if cfg.SignerKeyTimeout != 5*time.Second {
		t.Fatalf("SignerKeyTimeout = %v, want 5s", cfg.SignerKeyTimeout)
	}
	if _, err := cfg.SignerPolicyConfig(); err == nil {
		t.Fatalf("SignerPolicyConfig accepted an unconfigured policy")
	} else if !strings.Contains(err.Error(), EnvSignerChains) {
		t.Fatalf("SignerPolicyConfig error = %v, want it to name %s", err, EnvSignerChains)
	}
}

func TestSignerFullConfig(t *testing.T) {
	cfg, err := Load(fakeEnv(signerEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	pc, err := cfg.SignerPolicyConfig()
	if err != nil {
		t.Fatalf("SignerPolicyConfig() error = %v", err)
	}
	if len(pc.ChainIDs) != 1 || pc.ChainIDs[0] != 31337 {
		t.Fatalf("chains = %v", pc.ChainIDs)
	}
	if pc.MaxGasLimit != 100000 || pc.MaxAmount.String() != "2000000" {
		t.Fatalf("caps = %s/%d", pc.MaxAmount, pc.MaxGasLimit)
	}
	for _, want := range []string{"signer_http_addr=127.0.0.1:8091", "signer_mode=development"} {
		if !strings.Contains(cfg.Summary(), want) {
			t.Fatalf("summary %q lacks %q", cfg.Summary(), want)
		}
	}
}

func TestSignerMalformed(t *testing.T) {
	cases := []struct {
		name string
		env  string
		val  string
	}{
		{"addr", EnvSignerHTTPAddr, "not-an-addr"},
		{"mode", EnvSignerMode, "staging"},
		{"timeout", EnvSignerKeyTimeout, "soon"},
		{"zero timeout", EnvSignerKeyTimeout, "0s"},
		{"chains", EnvSignerChains, "31337,abc"},
		{"zero chain", EnvSignerChains, "0"},
		{"amount", EnvSignerMaxAmount, "1.5"},
		{"gas", EnvSignerMaxGasLimit, "-3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := signerEnv()
			env[tc.env] = tc.val
			if _, err := Load(fakeEnv(env)); err == nil {
				t.Fatalf("%s=%q accepted", tc.env, tc.val)
			} else if !strings.Contains(err.Error(), tc.env) {
				t.Fatalf("error %v does not name %s", err, tc.env)
			}
		})
	}
}
