package signer

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// validBody is a complete type-2 request per contracts/api.md §2.
func validBody() string {
	return `{
		"signing_request_id": "sr-7f3a",
		"attempt_id": "at-9c02",
		"intent_id": "pi-1b44",
		"binding_ref": "nb-1",
		"recovery_version": 12,
		"chain_id": 31337,
		"sender": "0x1111111111111111111111111111111111111111",
		"nonce": "42",
		"tx_type": 2,
		"to": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"value": "0",
		"data": "0xa9059cbb000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000000000000000000000000000000000000000000000000000000000f4240",
		"gas_limit": "65000",
		"max_fee_per_gas": "1500000000",
		"max_priority_fee_per_gas": "1000000000",
		"asset": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"recipient": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		"amount": "1000000",
		"authorization_id": "wa-1"
	}`
}

func classOf(t *testing.T, err error) RefusalClass {
	t.Helper()
	var re *RefusalError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *RefusalError", err)
	}
	return re.Class
}

func TestDecodeHappyPath(t *testing.T) {
	req, err := DecodeRequest([]byte(validBody()))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	if req.ChainID != 31337 || req.TxType != 2 || req.Nonce != "42" {
		t.Fatalf("decoded fields wrong: %+v", req)
	}
	h1, err := req.ContentHash()
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	h2, err := req.ContentHash()
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	if h1 != h2 || h1 == (common.Hash{}) {
		t.Fatalf("content hash not deterministic: %s vs %s", h1, h2)
	}

	changed := validBody()
	changed = strings.Replace(changed, `"1000000"`, `"1000001"`, 1)
	req2, err := DecodeRequest([]byte(changed))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	h3, err := req2.ContentHash()
	if err != nil {
		t.Fatalf("ContentHash: %v", err)
	}
	if h3 == h1 {
		t.Fatalf("content hash insensitive to amount change")
	}
}

func TestDigestOnlyRefused(t *testing.T) {
	for _, body := range []string{
		`{"digest": "0xdeadbeef"}`,
		`{"hash": "0xdeadbeef"}`,
		`{"message": "0xdeadbeef"}`,
		`{"signing_hash": "0xdeadbeef", "signing_request_id": "sr-1"}`,
		`{"raw_transaction": "0x02f8"}`,
	} {
		_, err := DecodeRequest([]byte(body))
		if err == nil {
			t.Fatalf("body %s accepted", body)
		}
		if got := classOf(t, err); got != ClassArbitraryDigestRejected {
			t.Fatalf("body %s class = %s, want arbitrary_digest_rejected", body, got)
		}
	}
}

func TestShapeRefusals(t *testing.T) {
	full := validBody()
	if _, err := DecodeRequest([]byte(full + " ")); err != nil {
		t.Fatalf("trailing whitespace must be fine: %v", err)
	}

	unknown := strings.Replace(full, `"wa-1"`, `"wa-1", "digest": "0x1"`, 1)
	if _, err := DecodeRequest([]byte(unknown)); err == nil {
		t.Fatalf("unknown field accepted")
	} else if got := classOf(t, err); got != ClassMalformedRequest {
		t.Fatalf("unknown field class = %s, want malformed_request", got)
	}

	missing := strings.Replace(full, `"binding_ref": "nb-1",`, ``, 1)
	if _, err := DecodeRequest([]byte(missing)); err == nil {
		t.Fatalf("missing field accepted")
	} else if got := classOf(t, err); got != ClassValidationFailed {
		t.Fatalf("missing field class = %s, want validation_failed", got)
	}

	wrongType := strings.Replace(full, `"nonce": "42"`, `"nonce": 42`, 1)
	if _, err := DecodeRequest([]byte(wrongType)); err == nil {
		t.Fatalf("wrong type accepted")
	} else if got := classOf(t, err); got != ClassMalformedRequest {
		t.Fatalf("wrong type class = %s, want malformed_request", got)
	}

	if _, err := DecodeRequest([]byte(`{"signing_request_id": "sr-1"`)); err == nil {
		t.Fatalf("truncated JSON accepted")
	} else if got := classOf(t, err); got != ClassMalformedRequest {
		t.Fatalf("truncated JSON class = %s, want malformed_request", got)
	}
}

func TestTransactionReconstruction(t *testing.T) {
	req, err := DecodeRequest([]byte(validBody()))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	tx, err := req.Transaction()
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if tx.Type() != 2 {
		t.Fatalf("tx type = %d, want 2", tx.Type())
	}
	if tx.Nonce() != 42 || tx.Gas() != 65000 {
		t.Fatalf("tx fields wrong: nonce=%d gas=%d", tx.Nonce(), tx.Gas())
	}
	if tx.GasFeeCap().Cmp(big.NewInt(1500000000)) != 0 {
		t.Fatalf("fee cap wrong: %s", tx.GasFeeCap())
	}
	if *tx.To() != common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") {
		t.Fatalf("to wrong: %s", tx.To())
	}

	legacy := strings.Replace(validBody(), `"tx_type": 2`, `"tx_type": 0`, 1)
	legacy = strings.Replace(legacy, `"max_fee_per_gas": "1500000000",`, `"gas_price": "1000000000",`, 1)
	legacy = strings.Replace(legacy, `"max_priority_fee_per_gas": "1000000000",`, ``, 1)
	req0, err := DecodeRequest([]byte(legacy))
	if err != nil {
		t.Fatalf("DecodeRequest legacy: %v", err)
	}
	tx0, err := req0.Transaction()
	if err != nil {
		t.Fatalf("Transaction legacy: %v", err)
	}
	if tx0.Type() != 0 {
		t.Fatalf("legacy tx type = %d, want 0", tx0.Type())
	}
}

func TestVerifySignatureRoundTrip(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey).Hex()
	body := strings.Replace(validBody(), "0x1111111111111111111111111111111111111111", sender, 1)
	req, err := DecodeRequest([]byte(body))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	tx, err := req.Transaction()
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	signer := types.LatestSignerForChainID(new(big.Int).SetUint64(req.ChainID))
	signed, err := types.SignTx(tx, signer, key)
	if err != nil {
		t.Fatalf("SignTx: %v", err)
	}
	v, r, s := signed.RawSignatureValues()
	sig := make([]byte, 65)
	r.FillBytes(sig[0:32])
	s.FillBytes(sig[32:64])
	sig[64] = byte(v.Uint64())
	from, txHash, err := req.VerifySignature(sig)
	if err != nil {
		t.Fatalf("VerifySignature: %v", err)
	}
	if from != common.HexToAddress(sender) {
		t.Fatalf("recovered %s, want %s", from, sender)
	}
	if txHash != signed.Hash() {
		t.Fatalf("tx hash %s, want %s", txHash, signed.Hash())
	}

	if _, _, err := req.VerifySignature(sig[:64]); err == nil {
		t.Fatalf("short signature accepted")
	}
	other, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	signed2, err := types.SignTx(tx, signer, other)
	if err != nil {
		t.Fatalf("SignTx: %v", err)
	}
	v2, r2, s2 := signed2.RawSignatureValues()
	sig2 := make([]byte, 65)
	r2.FillBytes(sig2[0:32])
	s2.FillBytes(sig2[32:64])
	sig2[64] = byte(v2.Uint64())
	if _, _, err := req.VerifySignature(sig2); err == nil {
		t.Fatalf("foreign signature accepted")
	} else if got := classOf(t, err); got != ClassValidationFailed {
		t.Fatalf("foreign signature class = %s, want validation_failed", got)
	}
}
