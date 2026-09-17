package signer

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/xtianxx/txharbor/internal/logx"
)

// secrecy_test.go owns spec task T025 (US5; FR-22, SC-05): zero key material
// and zero raw signed-transaction bytes on the refusal paths. At this layer the
// observability surface is the RefusalError (rendered into logs, metrics labels
// and HTTP responses by the serving layer), so the scan is:
//  1. every refusal error is free of the private key (hex or raw bytes) and of
//     any signed-transaction payload;
//  2. logx.Redact removes key material that operators might wrap in a
//     credential-labelled log line;
//  3. no production signer source embeds a 64-hex private-key-shaped literal.

// TestSecrecyRefusalErrorsCarryNoKeyMaterial drives the full refusal surface
// with a generated key and asserts none of the returned errors leaks the key
// or a previously signed transaction's bytes.
func TestSecrecyRefusalErrorsCarryNoKeyMaterial(t *testing.T) {
	hexKey, addr := devKeyHex(t)
	keyPath := writeTestKey(t, hexKey)
	keyBytes := common.FromHex(hexKey)

	p, err := NewDevKeyProvider(ModeDevelopment, keyPath, time.Second)
	if err != nil {
		t.Fatalf("NewDevKeyProvider: %v", err)
	}

	// Produce real signed-transaction bytes: they are the delivery payload and
	// must never reappear in a refusal.
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID: big.NewInt(31337), Nonce: 3,
		GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(2), Gas: 21000,
		To: &addr, Value: big.NewInt(0),
	})
	signed, err := p.SignTx(context.Background(), addr, big.NewInt(31337), tx)
	if err != nil {
		t.Fatalf("SignTx: %v", err)
	}
	rawSigned, err := signed.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	other, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	otherAddr := crypto.PubkeyToAddress(other.PublicKey)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	var refusals []error
	add := func(e error) {
		if e == nil {
			t.Fatalf("expected a refusal, got nil")
		}
		refusals = append(refusals, e)
	}
	if _, err := NewDevKeyProvider(ModeProduction, keyPath, time.Second); err != nil {
		add(err)
	} else {
		t.Fatalf("production accepted the local key")
	}
	_, err = NewDevKeyProvider(ModeDevelopment, "", time.Second)
	add(err)
	_, err = NewDevKeyProvider(ModeDevelopment, filepath.Join(t.TempDir(), "absent.key"), time.Second)
	add(err)
	_, err = NewDevKeyProvider(ModeDevelopment, writeTestKey(t, "not-a-key"), time.Second)
	add(err)
	_, err = p.SignTx(context.Background(), otherAddr, big.NewInt(31337), tx)
	add(err)
	_, err = p.SignTx(context.Background(), addr, big.NewInt(0), tx)
	add(err)
	_, err = p.SignTx(context.Background(), addr, big.NewInt(31337), nil)
	add(err)
	_, err = p.SignTx(expired, addr, big.NewInt(31337), tx)
	add(err)

	for i, e := range refusals {
		msg := e.Error()
		if strings.Contains(msg, hexKey) {
			t.Errorf("refusal #%d leaks the key hex: %q", i, msg)
		}
		if strings.Contains(msg, string(keyBytes)) {
			t.Errorf("refusal #%d leaks raw key bytes", i)
		}
		if strings.Contains(msg, common.Bytes2Hex(rawSigned)) {
			t.Errorf("refusal #%d leaks raw signed-tx bytes", i)
		}
		if redacted := logx.Redact(msg); strings.Contains(redacted, hexKey) {
			t.Errorf("logx.Redact(refusal #%d) still leaks the key hex: %q", i, redacted)
		}
	}
}

// TestSecrecyRedactCoversKeyMaterial proves the log redaction the serving layer
// relies on actually removes key material (and a signed payload) when an
// operator wraps it in a credential-labelled line.
func TestSecrecyRedactCoversKeyMaterial(t *testing.T) {
	hexKey, _ := devKeyHex(t)
	signedHex := common.Bytes2Hex(make([]byte, 32))
	cases := map[string]string{
		"private_key": "private_key=" + hexKey,
		"private-key": "private-key=0x" + hexKey,
		"token":       "token=" + hexKey,
		"api_key":     "api_key=" + hexKey,
		"secret":      "secret=" + hexKey,
		"signed tx":   "authorization=" + signedHex,
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			got := logx.Redact(line)
			if strings.Contains(got, hexKey) || strings.Contains(got, signedHex) {
				t.Fatalf("Redact(%q) = %q, leaked the secret", line, got)
			}
			if !strings.Contains(got, logx.Redacted) {
				t.Fatalf("Redact(%q) = %q, secret not replaced", line, got)
			}
		})
	}
}

// TestSecrecyRepoEmbedsNoKeyMaterial scans the production (non-test) signer
// sources for a 64-hex private-key-shaped literal. Generated test keys live
// only in memory; a committed key would fail here.
func TestSecrecyRepoEmbedsNoKeyMaterial(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	// Exactly 64 hex chars, not part of a longer word: private-key shape.
	keyLiteral := regexp.MustCompile(`\b[0-9a-fA-F]{64}\b`)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		if m := keyLiteral.Find(body); m != nil {
			t.Errorf("%s embeds a 64-hex private-key-shaped literal: %s", name, m)
		}
	}
}
