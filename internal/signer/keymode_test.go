package signer_test

import (
	"context"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/signer"
)

// keymode_test.go owns spec task T024 (US5; FR-20/FR-21, SC-05): test/deploy
// key separation. A deployment (production) must refuse startup/signing rather
// than silently fall back to a local test key, even when a valid key file is
// present; only explicit development mode may build the local provider, and
// then only with an explicit key file. This couples the config knobs
// (SignerMode/SignerKeyFile) to the provider guard so a config default change
// or a guard removal fails here.
//
// It lives in signer_test (external test package) so it can import
// internal/config without the config -> signer import cycle; production
// boundary checks stay in boundary_test.go.

func kmGenerateKey(t *testing.T) (hexKey string, addr common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return common.Bytes2Hex(crypto.FromECDSA(key)), crypto.PubkeyToAddress(key.PublicKey)
}

func kmWriteKey(t *testing.T, hexKey string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "signer.key")
	if err := os.WriteFile(path, []byte(hexKey), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func kmRefusalClass(t *testing.T, err error) signer.RefusalClass {
	t.Helper()
	var re *signer.RefusalError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *signer.RefusalError", err)
	}
	return re.Class
}

func TestKeyModeConfigMix(t *testing.T) {
	hexKey, addr := kmGenerateKey(t)
	keyFile := kmWriteKey(t, hexKey)

	// Deployment mode with test-key material on disk: startup/signing refuses.
	// The key file exists and is valid, so this is exactly the "would have
	// worked if it fell back" case.
	deploy := config.Config{
		SignerMode:       config.DefaultSignerMode,
		SignerKeyFile:    keyFile,
		SignerKeyTimeout: config.DefaultSignerKeyTimeout,
	}
	if deploy.SignerMode != "production" {
		t.Fatalf("default signer mode = %q, want production (fail-closed)", deploy.SignerMode)
	}
	_, err := signer.NewDevKeyProvider(signer.Mode(deploy.SignerMode), deploy.SignerKeyFile, deploy.SignerKeyTimeout)
	if err == nil {
		t.Fatalf("production mode silently accepted a local test key")
	}
	if got := kmRefusalClass(t, err); got != signer.ClassKeyProviderUnavailable {
		t.Fatalf("deploy refusal class = %q, want %q", got, signer.ClassKeyProviderUnavailable)
	}

	// Explicit development mode is the only path that builds the provider,
	// and it must bind to the file's key (proving the deploy refusal above is
	// separation, not a broken provider).
	dev := config.Config{
		SignerMode:       "development",
		SignerKeyFile:    keyFile,
		SignerKeyTimeout: time.Second,
	}
	p, err := signer.NewDevKeyProvider(signer.Mode(dev.SignerMode), dev.SignerKeyFile, dev.SignerKeyTimeout)
	if err != nil {
		t.Fatalf("development provider: %v", err)
	}
	if p.Address() != addr {
		t.Fatalf("provider bound to %s, want %s", p.Address(), addr)
	}
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID: big.NewInt(31337), Nonce: 1,
		GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2), Gas: 21000,
		To: &addr, Value: big.NewInt(0),
	})
	if _, err := p.SignTx(context.Background(), addr, big.NewInt(31337), tx); err != nil {
		t.Fatalf("development signing refused: %v", err)
	}
}

func TestKeyModeRefusesMixedOrIncompleteConfig(t *testing.T) {
	hexKey, _ := kmGenerateKey(t)
	keyFile := kmWriteKey(t, hexKey)

	cases := map[string]config.Config{
		// Deploy mode must refuse even with a key file handed to it.
		"production with key file": {SignerMode: "production", SignerKeyFile: keyFile, SignerKeyTimeout: time.Second},
		// Unknown mode must not degrade to development.
		"unknown mode": {SignerMode: "staging", SignerKeyFile: keyFile, SignerKeyTimeout: time.Second},
		// Development without an explicit file has no implicit default.
		"development without key file": {SignerMode: "development", SignerKeyFile: "", SignerKeyTimeout: time.Second},
		// A configured-but-unreadable file must not fall back either.
		"development missing file": {SignerMode: "development", SignerKeyFile: filepath.Join(t.TempDir(), "absent.key"), SignerKeyTimeout: time.Second},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := signer.NewDevKeyProvider(signer.Mode(cfg.SignerMode), cfg.SignerKeyFile, cfg.SignerKeyTimeout)
			if err == nil {
				t.Fatalf("provider accepted a mixed/incomplete config")
			}
			if got := kmRefusalClass(t, err); got != signer.ClassKeyProviderUnavailable {
				t.Fatalf("refusal class = %q, want %q", got, signer.ClassKeyProviderUnavailable)
			}
		})
	}
}
