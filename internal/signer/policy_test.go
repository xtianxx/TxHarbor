package signer

import (
	"math/big"
	"strings"
	"testing"
)

func testPolicy(t *testing.T) *Policy {
	t.Helper()
	p, err := NewPolicy(PolicyConfig{
		ChainIDs:             []int64{31337},
		Senders:              []string{"0x1111111111111111111111111111111111111111"},
		Assets:               []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Recipients:           []string{"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		MaxAmount:            big.NewInt(2000000),
		MaxGasLimit:          100000,
		MaxFeePerGas:         big.NewInt(2000000000),
		MaxPriorityFeePerGas: big.NewInt(1500000000),
		MaxGasPrice:          big.NewInt(2000000000),
	})
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	return p
}

func expectPolicyField(t *testing.T, p *Policy, req Request, field string) {
	t.Helper()
	err := p.Check(&req)
	if err == nil {
		t.Fatalf("policy accepted %s", field)
	}
	re, ok := err.(*RefusalError)
	if !ok {
		t.Fatalf("policy error %v is not a *RefusalError", err)
	}
	if re.Class != ClassPolicyRefused || re.Field != field {
		t.Fatalf("got %s/%s, want policy_refused/%s", re.Class, re.Field, field)
	}
	if !strings.HasPrefix(re.Msg, "policy_") {
		t.Fatalf("policy detail %q lacks policy_ prefix", re.Msg)
	}
}

func TestPolicyHappyPathAndVersion(t *testing.T) {
	p := testPolicy(t)
	req := mustDecode(t, validBody())
	if err := p.Check(&req); err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !strings.HasPrefix(p.Version(), "0x") || len(p.Version()) != 66 {
		t.Fatalf("version hash malformed: %q", p.Version())
	}

	again, err := NewPolicy(PolicyConfig{
		ChainIDs:             []int64{31337},
		Senders:              []string{"0x1111111111111111111111111111111111111111"},
		Assets:               []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Recipients:           []string{"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		MaxAmount:            big.NewInt(2000000),
		MaxGasLimit:          100000,
		MaxFeePerGas:         big.NewInt(2000000000),
		MaxPriorityFeePerGas: big.NewInt(1500000000),
		MaxGasPrice:          big.NewInt(2000000000),
	})
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	if again.Version() != p.Version() {
		t.Fatalf("equivalent policies hash different")
	}

	changed, err := NewPolicy(PolicyConfig{
		ChainIDs:             []int64{31337},
		Senders:              []string{"0x1111111111111111111111111111111111111111"},
		Assets:               []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Recipients:           []string{"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		MaxAmount:            big.NewInt(2000001),
		MaxGasLimit:          100000,
		MaxFeePerGas:         big.NewInt(2000000000),
		MaxPriorityFeePerGas: big.NewInt(1500000000),
		MaxGasPrice:          big.NewInt(2000000000),
	})
	if err != nil {
		t.Fatalf("NewPolicy: %v", err)
	}
	if changed.Version() == p.Version() {
		t.Fatalf("changed policy hashes equal")
	}
}

func TestPolicyRefusals(t *testing.T) {
	p := testPolicy(t)
	base := validBody()
	cases := []struct {
		name  string
		field string
		mut   func(string) string
	}{
		{"chain not permitted", "chain_id", func(b string) string {
			return strings.Replace(b, `"chain_id": 31337`, `"chain_id": 1`, 1)
		}},
		{"sender not permitted", "sender", func(b string) string {
			return strings.Replace(b, "0x1111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222", 1)
		}},
		{"asset not permitted", "asset", func(b string) string {
			b = strings.Replace(b, `"to": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`, `"to": "0xdddddddddddddddddddddddddddddddddddddddd"`, 1)
			return strings.Replace(b, `"asset": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`, `"asset": "0xdddddddddddddddddddddddddddddddddddddddd"`, 1)
		}},
		{"amount over cap", "amount", func(b string) string {
			b = strings.Replace(b, `"amount": "1000000"`, `"amount": "3000000"`, 1)
			return strings.Replace(b, "00000000000000000000000000000000000000000000000000000000000f4240", "00000000000000000000000000000000000000000000000000000000002dc6c0", 1)
		}},
		{"gas over cap", "gas_limit", func(b string) string {
			return strings.Replace(b, `"gas_limit": "65000"`, `"gas_limit": "200000"`, 1)
		}},
		{"fee over cap", "max_fee_per_gas", func(b string) string {
			return strings.Replace(b, `"max_fee_per_gas": "1500000000"`, `"max_fee_per_gas": "3000000000"`, 1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := mustDecode(t, tc.mut(base))
			if err := Validate(&req); err != nil {
				t.Fatalf("shape must pass for policy test: %v", err)
			}
			expectPolicyField(t, p, req, tc.field)
		})
	}

	// Recipient mismatch also breaks calldata binding, so it is covered by
	// TestValidateCalldataBinding; here the policy path is checked directly.
	req := mustDecode(t, base)
	if err := Validate(&req); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := p.Check(&req); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

func TestPolicyConstructionRefuses(t *testing.T) {
	good := PolicyConfig{
		ChainIDs:             []int64{31337},
		Senders:              []string{"0x1111111111111111111111111111111111111111"},
		Assets:               []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Recipients:           []string{"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		MaxAmount:            big.NewInt(2000000),
		MaxGasLimit:          100000,
		MaxFeePerGas:         big.NewInt(2000000000),
		MaxPriorityFeePerGas: big.NewInt(1500000000),
		MaxGasPrice:          big.NewInt(2000000000),
	}
	bad := good
	bad.Recipients = nil
	if _, err := NewPolicy(bad); err == nil {
		t.Fatalf("empty recipients accepted")
	}
	bad = good
	bad.MaxAmount = big.NewInt(0)
	if _, err := NewPolicy(bad); err == nil {
		t.Fatalf("zero max amount accepted")
	}
}
