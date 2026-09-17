//go:build integration

// policy_integration_test.go owns spec task T022 for 009-signer-service: the V5
// validation-matrix integration test on a real, isolated PostgreSQL (quickstart
// V5; FR-06–FR-12; SC-04). Every out-of-policy field — chain, sender, asset,
// calldata, recipient, amount, fee shape — is refused before any signing path
// runs (zero signature_results rows), and the refusal evidence is persisted to
// signing_request_audit carrying the refusal class and the policy_version that
// decided it (data-model.md Tables 3/5; FR-12). The submit path (submit.go) is
// not required: validation and policy are driven directly and the audit pin is
// probed with raw SQL, exactly the single-statement pre-transaction append
// Table 5 allows for input-shape refusals.
package signer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// signerPolicyHTTPStatus maps a refusal class to the HTTP status fixed by
// contracts/api.md §2. The signer serving layer owns this mapping (T016); V5
// asserts every validation/policy violation lands in the 4xx pre-sign band
// (422 for shape and policy, 400 for a body that never parses).
func signerPolicyHTTPStatus(class RefusalClass) int {
	switch class {
	case ClassMalformedRequest, ClassArbitraryDigestRejected:
		return 400
	case ClassUnauthenticated:
		return 401
	case ClassSigningNotPermitted, ClassAuthorizationInvalid, ClassAuthorizationExpired,
		ClassAuthorizationRevoked, ClassAuthorizationUnverifiable:
		return 403
	case ClassValidationFailed, ClassPolicyRefused:
		return 422
	default:
		return 500
	}
}

// signerPolicyV5Refusal asserts err is a refusal of wantClass naming wantField.
func signerPolicyV5Refusal(t *testing.T, err error, wantClass RefusalClass, wantField string) *RefusalError {
	t.Helper()
	var re *RefusalError
	if !errors.As(err, &re) {
		t.Fatalf("error %v is not a *RefusalError", err)
	}
	if re.Class != wantClass || re.Field != wantField {
		t.Fatalf("refusal = %s/%s (%v), want %s/%s", re.Class, re.Field, err, wantClass, wantField)
	}
	return re
}

// signerPolicyV5AppendAudit records one refusal as the pre-transaction
// single-statement append Table 5 allows, returning its audit_id.
func signerPolicyV5AppendAudit(t *testing.T, pool *pgxpool.Pool, requestID string, callerID int64, class RefusalClass, detail string) int64 {
	t.Helper()
	var auditID int64
	err := pool.QueryRow(context.Background(), `INSERT INTO signing_request_audit
		(signing_request_id, caller_id, action, reason_class, detail)
		VALUES ($1, $2, 'rejected', $3, $4)
		RETURNING audit_id`, requestID, callerID, string(class), detail).Scan(&auditID)
	if err != nil {
		t.Fatalf("append refusal audit: %v", err)
	}
	return auditID
}

func signerPolicyV5ReadAudit(t *testing.T, pool *pgxpool.Pool, auditID int64) (string, string) {
	t.Helper()
	var class, detail string
	if err := pool.QueryRow(context.Background(),
		`SELECT reason_class, detail FROM signing_request_audit WHERE audit_id = $1`,
		auditID).Scan(&class, &detail); err != nil {
		t.Fatalf("read refusal audit %d: %v", auditID, err)
	}
	return class, detail
}

// TestSignerPolicyValidationMatrixIntegration is V5: the full negative matrix
// (chain/sender/asset/calldata/recipient/amount/fee-shape) refuses 422 before
// signing with zero signatures, records the class, and pins the deciding
// policy_version in the audit evidence; in-policy shapes pass both gates.
func TestSignerPolicyValidationMatrixIntegration(t *testing.T) {
	pool := signerAuthStartPG(t)
	const callerID = int64(4001)
	p := testPolicy(t)
	pinned := p.Version()
	if !strings.HasPrefix(pinned, "0x") || len(pinned) != 66 {
		t.Fatalf("pinned policy version %q is not an 0x 32-byte hash", pinned)
	}
	if p.Version() != pinned {
		t.Fatal("policy version is not stable across calls")
	}
	// A changed policy hashes differently, so the audit pin identifies the
	// deciding policy rather than any policy.
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
		t.Fatalf("NewPolicy(changed): %v", err)
	}
	if changed.Version() == pinned {
		t.Fatal("a changed policy hashed to the same version; the audit pin would be meaningless")
	}

	base := validBody()
	amountWord := func(n *big.Int) string { return fmt.Sprintf("%064x", n) }
	baseAmountWord := amountWord(big.NewInt(1000000))
	baseRecipientWord := "000000000000000000000000" + strings.Repeat("bb", 20)
	otherRecipientWord := "000000000000000000000000" + strings.Repeat("cc", 20)

	cases := []struct {
		name  string
		class RefusalClass
		field string
		mut   func(string) string
	}{
		{"chain_id zero", ClassValidationFailed, "chain_id", func(b string) string {
			return strings.Replace(b, `"chain_id": 31337`, `"chain_id": 0`, 1)
		}},
		{"chain_id not permitted", ClassPolicyRefused, "chain_id", func(b string) string {
			return strings.Replace(b, `"chain_id": 31337`, `"chain_id": 1`, 1)
		}},
		{"sender not hex", ClassValidationFailed, "sender", func(b string) string {
			return strings.Replace(b, "0x1111111111111111111111111111111111111111", "0xZZZZ", 1)
		}},
		{"sender not permitted", ClassPolicyRefused, "sender", func(b string) string {
			return strings.Replace(b, "0x1111111111111111111111111111111111111111",
				"0x2222222222222222222222222222222222222222", 1)
		}},
		{"asset differs from to", ClassValidationFailed, "asset", func(b string) string {
			return strings.Replace(b, `"asset": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
				`"asset": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`, 1)
		}},
		{"asset not permitted", ClassPolicyRefused, "asset", func(b string) string {
			b = strings.Replace(b, `"to": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
				`"to": "0xdddddddddddddddddddddddddddddddddddddddd"`, 1)
			return strings.Replace(b, `"asset": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`,
				`"asset": "0xdddddddddddddddddddddddddddddddddddddddd"`, 1)
		}},
		{"calldata wrong selector", ClassValidationFailed, "data", func(b string) string {
			return strings.Replace(b, "0xa9059cbb", "0xdeadbeef", 1)
		}},
		{"calldata recipient mismatch", ClassValidationFailed, "data", func(b string) string {
			return strings.Replace(b, `"recipient": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`,
				`"recipient": "0xcccccccccccccccccccccccccccccccccccccccc"`, 1)
		}},
		{"calldata amount mismatch", ClassValidationFailed, "data", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "1000001"`, 1)
		}},
		{"calldata not hex", ClassValidationFailed, "data", func(b string) string {
			return strings.Replace(b, `"data": "0xa9059cbb`, `"data": "zz`, 1)
		}},
		{"recipient not permitted", ClassPolicyRefused, "recipient", func(b string) string {
			b = strings.Replace(b, baseRecipientWord, otherRecipientWord, 1)
			return strings.Replace(b, `"recipient": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`,
				`"recipient": "0xcccccccccccccccccccccccccccccccccccccccc"`, 1)
		}},
		{"amount zero", ClassValidationFailed, "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "0"`, 1)
		}},
		{"amount negative", ClassValidationFailed, "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "-1"`, 1)
		}},
		{"amount decimal", ClassValidationFailed, "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "1.5"`, 1)
		}},
		{"amount non-integer", ClassValidationFailed, "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "0x10"`, 1)
		}},
		{"amount over uint256", ClassValidationFailed, "amount", func(b string) string {
			return strings.Replace(b, `"amount": "1000000"`, `"amount": "`+signerOverUint256+`"`, 1)
		}},
		{"amount over policy cap", ClassPolicyRefused, "amount", func(b string) string {
			b = strings.Replace(b, `"amount": "1000000"`, `"amount": "3000000"`, 1)
			return strings.Replace(b, baseAmountWord, amountWord(big.NewInt(3000000)), 1)
		}},
		{"native value nonzero", ClassValidationFailed, "value", func(b string) string {
			return strings.Replace(b, `"value": "0"`, `"value": "1"`, 1)
		}},
		{"gas_limit zero", ClassValidationFailed, "gas_limit", func(b string) string {
			return strings.Replace(b, `"gas_limit": "65000"`, `"gas_limit": "0"`, 1)
		}},
		{"gas_limit over cap", ClassPolicyRefused, "gas_limit", func(b string) string {
			return strings.Replace(b, `"gas_limit": "65000"`, `"gas_limit": "200000"`, 1)
		}},
		{"fee inversion", ClassValidationFailed, "max_priority_fee_per_gas", func(b string) string {
			return strings.Replace(b, `"max_priority_fee_per_gas": "1000000000"`,
				`"max_priority_fee_per_gas": "2000000000"`, 1)
		}},
		{"type 2 with gas_price", ClassValidationFailed, "gas_price", func(b string) string {
			return strings.Replace(b, `"max_priority_fee_per_gas": "1000000000",`,
				`"max_priority_fee_per_gas": "1000000000", "gas_price": "1",`, 1)
		}},
		{"type 0 with max fees", ClassValidationFailed, "gas_price", func(b string) string {
			b = strings.Replace(b, `"tx_type": 2`, `"tx_type": 0`, 1)
			return strings.Replace(b, `"max_priority_fee_per_gas": "1000000000",`,
				`"max_priority_fee_per_gas": "1000000000", "gas_price": "1",`, 1)
		}},
		{"fee shape missing", ClassValidationFailed, "gas_price", func(b string) string {
			b = strings.Replace(b, "\t\t\"max_fee_per_gas\": \"1500000000\",\n", "", 1)
			return strings.Replace(b, "\t\t\"max_priority_fee_per_gas\": \"1000000000\",\n", "", 1)
		}},
		{"type 0 gas_price negative", ClassValidationFailed, "gas_price", func(b string) string {
			b = strings.Replace(b, `"tx_type": 2`, `"tx_type": 0`, 1)
			b = strings.Replace(b, `"max_fee_per_gas": "1500000000",`, `"gas_price": "-1",`, 1)
			return strings.Replace(b, "\t\t\"max_priority_fee_per_gas\": \"1000000000\",\n", "", 1)
		}},
		{"max_fee_per_gas over cap", ClassPolicyRefused, "max_fee_per_gas", func(b string) string {
			return strings.Replace(b, `"max_fee_per_gas": "1500000000"`, `"max_fee_per_gas": "3000000000"`, 1)
		}},
		{"max_priority_fee_per_gas over cap", ClassPolicyRefused, "max_priority_fee_per_gas", func(b string) string {
			b = strings.Replace(b, `"max_fee_per_gas": "1500000000"`, `"max_fee_per_gas": "1600000000"`, 1)
			return strings.Replace(b, `"max_priority_fee_per_gas": "1000000000"`,
				`"max_priority_fee_per_gas": "1600000000"`, 1)
		}},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, decodeErr := DecodeRequest([]byte(tc.mut(base)))
			var re *RefusalError
			switch {
			case decodeErr != nil:
				re = signerPolicyV5Refusal(t, decodeErr, tc.class, tc.field)
			default:
				if verr := Validate(&req); verr != nil {
					re = signerPolicyV5Refusal(t, verr, tc.class, tc.field)
				} else {
					re = signerPolicyV5Refusal(t, p.Check(&req), tc.class, tc.field)
				}
			}
			if got := signerPolicyHTTPStatus(re.Class); got != 422 {
				t.Fatalf("refusal %s maps to HTTP %d, want 422 in the pre-sign 4xx band", re.Class, got)
			}
			detail := "policy_version=" + pinned
			auditID := signerPolicyV5AppendAudit(t, pool, fmt.Sprintf("rej-v5-%02d", i), callerID, re.Class, detail)
			class, gotDetail := signerPolicyV5ReadAudit(t, pool, auditID)
			if class != string(re.Class) {
				t.Fatalf("audit reason_class = %q, want %q", class, re.Class)
			}
			if gotDetail != detail {
				t.Fatalf("audit detail = %q, want the pinned policy version %q", gotDetail, detail)
			}
			if n := signerAuthSignatureCount(t, pool); n != 0 {
				t.Fatalf("refusal produced %d signature result(s), want 0", n)
			}
		})
	}

	positives := map[string]string{
		"type 2": base,
		"type 0": func() string {
			b := strings.Replace(base, `"tx_type": 2`, `"tx_type": 0`, 1)
			b = strings.Replace(b, `"max_fee_per_gas": "1500000000",`, `"gas_price": "1500000000",`, 1)
			return strings.Replace(b, "\t\t\"max_priority_fee_per_gas\": \"1000000000\",\n", "", 1)
		}(),
	}
	for name, body := range positives {
		t.Run("positive "+name, func(t *testing.T) {
			req, err := DecodeRequest([]byte(body))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := Validate(&req); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if err := p.Check(&req); err != nil {
				t.Fatalf("Check: %v", err)
			}
		})
	}
	if n := signerAuthSignatureCount(t, pool); n != 0 {
		t.Fatalf("validation matrix produced %d signature result(s), want 0", n)
	}
}
