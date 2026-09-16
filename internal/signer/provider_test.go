package signer

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// writeTestKey writes 32 bytes of hex to a temp file and returns its path.
func writeTestKey(t *testing.T, hexKey string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.key")
	if err := os.WriteFile(path, []byte(hexKey), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// devKeyHex returns fresh dev-only key hex (generated, never a real key).
func devKeyHex(t *testing.T) (string, common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return common.Bytes2Hex(crypto.FromECDSA(key)), crypto.PubkeyToAddress(key.PublicKey)
}

func TestKeyTimeoutFromEnv(t *testing.T) {
	get := func(v string) func(string) string {
		return func(string) string { return v }
	}
	d, err := KeyTimeoutFromEnv(get(""))
	if err != nil || d != 5*time.Second {
		t.Fatalf("unset = %v, %v; want 5s", d, err)
	}
	d, err = KeyTimeoutFromEnv(get("250ms"))
	if err != nil || d != 250*time.Millisecond {
		t.Fatalf("250ms = %v, %v", d, err)
	}
	for _, bad := range []string{"0s", "-1s", "soon", "5"} {
		if _, err := KeyTimeoutFromEnv(get(bad)); err == nil {
			t.Fatalf("timeout %q accepted", bad)
		}
	}
}

func TestModeGuard(t *testing.T) {
	hexKey, _ := devKeyHex(t)
	path := writeTestKey(t, hexKey)
	if _, err := NewDevKeyProvider(ModeProduction, path, time.Second); err == nil {
		t.Fatalf("production accepted a local key provider")
	}
	if _, err := NewDevKeyProvider("staging", path, time.Second); err == nil {
		t.Fatalf("unknown mode accepted")
	}
	if _, err := NewDevKeyProvider(ModeDevelopment, "", time.Second); err == nil {
		t.Fatalf("development accepted an empty key file")
	}
	if _, err := NewDevKeyProvider(ModeDevelopment, filepath.Join(t.TempDir(), "missing.key"), time.Second); err == nil {
		t.Fatalf("missing key file accepted")
	}
	if _, err := NewDevKeyProvider(ModeDevelopment, writeTestKey(t, "not-hex"), time.Second); err == nil {
		t.Fatalf("garbage key file accepted")
	}
}

func TestDevSignRoundTrip(t *testing.T) {
	hexKey, addr := devKeyHex(t)
	p, err := NewDevKeyProvider(ModeDevelopment, writeTestKey(t, hexKey), time.Second)
	if err != nil {
		t.Fatalf("NewDevKeyProvider: %v", err)
	}
	if p.Address() != addr {
		t.Fatalf("bound address wrong")
	}
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID: big.NewInt(31337), Nonce: 7,
		GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2), Gas: 21000,
		To: &addr, Value: big.NewInt(0),
	})
	signed, err := p.SignTx(context.Background(), addr, big.NewInt(31337), tx)
	if err != nil {
		t.Fatalf("SignTx: %v", err)
	}
	from, err := types.Sender(types.LatestSignerForChainID(big.NewInt(31337)), signed)
	if err != nil || from != addr {
		t.Fatalf("recovered %s (%v), want %s", from, err, addr)
	}

	other, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	otherAddr := crypto.PubkeyToAddress(other.PublicKey)
	if _, err := p.SignTx(context.Background(), otherAddr, big.NewInt(31337), tx); err == nil {
		t.Fatalf("foreign sender accepted")
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := p.SignTx(ctx, addr, big.NewInt(31337), tx); err == nil {
		t.Fatalf("expired context accepted")
	} else if re, ok := err.(*RefusalError); !ok || re.Class != ClassKeyProviderTimeout {
		t.Fatalf("expired context class = %v, want key_provider_timeout", err)
	}

	if _, err := p.SignTx(context.Background(), addr, big.NewInt(31337), nil); err == nil {
		t.Fatalf("nil transaction accepted")
	}
}
