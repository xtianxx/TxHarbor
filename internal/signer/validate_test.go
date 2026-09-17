package signer

import (
	"strings"
	"testing"
)

func mustDecode(t *testing.T, body string) Request {
	t.Helper()
	req, err := DecodeRequest([]byte(body))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	return req
}

func expectField(t *testing.T, req Request, field string) {
	t.Helper()
	err := Validate(&req)
	if err == nil {
		t.Fatalf("field %s accepted", field)
	}
	re, ok := err.(*RefusalError)
	if !ok {
		t.Fatalf("field %s error %v is not a *RefusalError", field, err)
	}
	if re.Class != ClassValidationFailed || re.Field != field {
		t.Fatalf("field %s got %s/%s, want validation_failed/%s", field, re.Class, re.Field, field)
	}
}

func TestValidateHappyPath(t *testing.T) {
	req := mustDecode(t, validBody())
	if err := Validate(&req); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateMatrix(t *testing.T) {
	base := validBody()
	cases := []struct {
		name  string
		field string
		mut   func(string) string
	}{
		{"zero chain", "chain_id", func(b string) string {
			return strings.Replace(b, `"chain_id": 31337`, `"chain_id": 0`, 1)
		}},
		{"bad tx type", "tx_type", func(b string) string {
			return strings.Replace(b, `"tx_type": 2`, `"tx_type": 1`, 1)
		}},
		{"bad sender hex", "sender", func(b string) string {
			return strings.Replace(b, "0x1111111111111111111111111111111111111111", "0xZZZZ", 1)
		}},
		{"bad EIP-55 checksum", "to", func(b string) string {
			return strings.Replace(b, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0x5aaeb6053F3E94C9b9A09f33669435E7Ef1BeAed", 1)
		}},
		{"asset differs from to", "asset", func(b string) string {
			return strings.Replace(b, `"asset": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`, `"asset": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`, 1)
		}},
		{"negative nonce", "nonce", func(b string) string {
			return strings.Replace(b, `"nonce": "42"`, `"nonce": "-1"`, 1)
		}},
		{"float amount", "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "1.5"`, 1)
		}},
		{"hex amount", "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "0x10"`, 1)
		}},
		{"nonzero value", "value", func(b string) string {
			return strings.Replace(b, `"value": "0"`, `"value": "1"`, 1)
		}},
		{"zero gas", "gas_limit", func(b string) string {
			return strings.Replace(b, `"gas_limit": "65000"`, `"gas_limit": "0"`, 1)
		}},
		{"fee inversion", "max_priority_fee_per_gas", func(b string) string {
			return strings.Replace(b, `"max_priority_fee_per_gas": "1000000000"`, `"max_priority_fee_per_gas": "2000000000"`, 1)
		}},
		{"gas price on type 2", "gas_price", func(b string) string {
			return strings.Replace(b, `"max_priority_fee_per_gas": "1000000000",`, `"max_priority_fee_per_gas": "1000000000", "gas_price": "1",`, 1)
		}},
		{"wrong selector", "data", func(b string) string {
			return strings.Replace(b, "0xa9059cbb", "0xdeadbeef", 1)
		}},
		{"short calldata", "data", func(b string) string {
			return strings.Replace(b, "00000000000000000000000000000000000000000000000000000000000f4240", "00f4240", 1)
		}},
		{"non-hex data", "data", func(b string) string {
			return strings.Replace(b, `"data": "0xa9059cbb`, `"data": "zz`, 1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expectField(t, mustDecode(t, tc.mut(base)), tc.field)
		})
	}
}

func TestValidateEIP55MixedCaseAccepted(t *testing.T) {
	base := validBody()
	mixed := strings.Replace(base, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", 2)
	req := mustDecode(t, mixed)
	if err := Validate(&req); err != nil {
		t.Fatalf("valid EIP-55 mixed-case refused: %v", err)
	}
}

func TestValidateCalldataBinding(t *testing.T) {
	base := validBody()
	otherRecipient := `"recipient": "0xcccccccccccccccccccccccccccccccccccccccc"`
	req := mustDecode(t, strings.Replace(base, `"recipient": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`, otherRecipient, 1))
	expectField(t, req, "data")

	otherAmount := `"amount": "1000001"`
	req2 := mustDecode(t, strings.Replace(base, `"amount": "1000000"`, otherAmount, 1))
	expectField(t, req2, "data")
}
