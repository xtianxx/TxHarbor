package withdrawal

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// Known EIP-55 test vector (mixed case) and its pure-case forms.
const (
	checksummedAddr = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
	lowerAddr       = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"
	upperAddr       = "0x5AAEB6053F3E94C9B9A09F33669435E7EF1BEAED"
	badChecksumAddr = "0x5Aaeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
	otherValidAddr  = "0x52908400098527886E0F7030069857D2E4169EE7"
)

func requireValidationError(t *testing.T, err error, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want *Error with field %q", field)
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error type = %T, want *Error", err)
	}
	if e.Code != CodeValidationFailed {
		t.Errorf("Code = %q, want %q", e.Code, CodeValidationFailed)
	}
	if e.Field != field {
		t.Errorf("Field = %q, want %q", e.Field, field)
	}
}

func TestValidateChainID(t *testing.T) {
	const expected = int64(31337)

	t.Run("matching positive chain is accepted", func(t *testing.T) {
		// Given this deployment is bound to chain 31337
		// When a request carries chain_id 31337
		// Then it is accepted
		if err := ValidateChainID(expected, expected); err != nil {
			t.Fatalf("ValidateChainID(%d, %d) = %v, want nil", expected, expected, err)
		}
	})

	t.Run("zero chain is rejected", func(t *testing.T) {
		requireValidationError(t, ValidateChainID(0, expected), "chain_id")
	})

	t.Run("negative chain is rejected", func(t *testing.T) {
		requireValidationError(t, ValidateChainID(-1, expected), "chain_id")
	})

	t.Run("mismatched chain is rejected", func(t *testing.T) {
		requireValidationError(t, ValidateChainID(1, expected), "chain_id")
	})

	t.Run("positive chain unequal to expected is rejected", func(t *testing.T) {
		requireValidationError(t, ValidateChainID(expected, 0), "chain_id")
	})
}

func TestValidateAssetWhitelisted(t *testing.T) {
	allowlist := []string{lowerAddr, "0x1111111111111111111111111111111111111111"}

	t.Run("canonical member is accepted", func(t *testing.T) {
		if err := ValidateAssetWhitelisted(lowerAddr, allowlist); err != nil {
			t.Fatalf("ValidateAssetWhitelisted(lower) = %v, want nil", err)
		}
	})

	t.Run("non-member is rejected", func(t *testing.T) {
		requireValidationError(t,
			ValidateAssetWhitelisted("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", allowlist),
			"asset")
	})

	t.Run("empty allowlist rejects and never falls back to full chain", func(t *testing.T) {
		requireValidationError(t, ValidateAssetWhitelisted(lowerAddr, nil), "asset")
		requireValidationError(t, ValidateAssetWhitelisted(lowerAddr, []string{}), "asset")
	})
}

func TestValidateAmountRejectsBadShape(t *testing.T) {
	bad := []struct {
		name string
		raw  string
	}{
		{"zero", "0"},
		{"leading zero", "00123"},
		{"explicit plus", "+123"},
		{"negative", "-5"},
		{"decimal", "1.5"},
		{"exponent", "1e3"},
		{"non-digit", "abc"},
		{"empty", ""},
		{"leading space", " 12"},
		{"trailing space", "12 "},
		{"tab", "12\t"},
		{"newline", "12\n"},
		{"hex literal", "0x10"},
		{"digit separators", "1_000"},
		{"bare plus", "+"},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			// Given a transport amount that is not [1-9][0-9]*
			// When ValidateAmount parses it
			// Then it is rejected with field "amount" and no value escapes
			v, err := ValidateAmount(tc.raw)
			if v != nil {
				t.Errorf("value = %v, want nil (zero side effects)", v)
			}
			requireValidationError(t, err, "amount")
		})
	}
}

func TestValidateAmountAcceptsExactInteger(t *testing.T) {
	good := []struct {
		name string
		raw  string
	}{
		{"one", "1"},
		{"ten", "10"},
		{"typical wei", "1230000000000000000"},
		{"uint256 max minus one", "115792089237316195423570985008687907853269984665640564039457584007913129639934"},
		{"uint256 max", maxUint256Dec},
	}

	for _, tc := range good {
		t.Run(tc.name, func(t *testing.T) {
			// Given a transport amount in [1-9][0-9]* and ≤ 2²⁵⁶−1
			// When ValidateAmount parses it
			// Then it returns the exact integer value
			v, err := ValidateAmount(tc.raw)
			if err != nil {
				t.Fatalf("ValidateAmount(%q) error = %v, want nil", tc.raw, err)
			}
			if v == nil {
				t.Fatalf("ValidateAmount(%q) value = nil, want parsed integer", tc.raw)
			}
			want, ok := new(big.Int).SetString(tc.raw, 10)
			if !ok {
				t.Fatalf("test vector %q is not a base-10 integer", tc.raw)
			}
			if v.Cmp(want) != 0 {
				t.Errorf("value = %s, want %s", v.String(), want.String())
			}
		})
	}
}

func TestValidateAmountUint256Boundary(t *testing.T) {
	t.Run("max is accepted and equals the computed 2^256-1", func(t *testing.T) {
		// Given the literal max-uint256 decimal string
		// When parsed and compared to the independently computed (1<<256)-1
		// Then both bounds agree and the value is returned exactly
		v, err := ValidateAmount(maxUint256Dec)
		if err != nil {
			t.Fatalf("ValidateAmount(max) error = %v, want nil", err)
		}
		if v.Cmp(maxUint256) != 0 {
			t.Errorf("max value = %s, want computed %s", v.String(), maxUint256.String())
		}
	})

	t.Run("max plus one is rejected with no value", func(t *testing.T) {
		// Given 2^256 = max + 1
		// When parsed
		// Then it is rejected (layer-2 range check) and no value escapes
		const maxPlusOne = "115792089237316195423570985008687907853269984665640564039457584007913129639936"
		if got := new(big.Int).Add(maxUint256, big.NewInt(1)).String(); got != maxPlusOne {
			t.Fatalf("2^256 = %s, want %s", got, maxPlusOne)
		}
		v, err := ValidateAmount(maxPlusOne)
		if v != nil {
			t.Errorf("value = %v, want nil (zero side effects)", v)
		}
		requireValidationError(t, err, "amount")
	})

	t.Run("over-length 78-digit value above max is rejected", func(t *testing.T) {
		v, err := ValidateAmount(strings.Repeat("9", 78))
		if v != nil {
			t.Errorf("value = %v, want nil", v)
		}
		requireValidationError(t, err, "amount")
	})

	t.Run("over-length 100-digit value dies in the range check", func(t *testing.T) {
		// Given a 100-digit digit string (exactly representable by big.Int)
		// When parsed
		// Then the big.Int range check rejects it before any DB boundary
		v, err := ValidateAmount(strings.Repeat("9", 100))
		if v != nil {
			t.Errorf("value = %v, want nil", v)
		}
		requireValidationError(t, err, "amount")
	})
}

func TestCanonicalAddress(t *testing.T) {
	accept := []struct {
		name string
		raw  string
		want string
	}{
		{"all lowercase stays lowercase", lowerAddr, lowerAddr},
		{"all uppercase canonicalizes to lowercase", upperAddr, lowerAddr},
		{"valid EIP-55 mixed case canonicalizes to lowercase", checksummedAddr, lowerAddr},
		{"second valid EIP-55 vector", otherValidAddr, strings.ToLower(otherValidAddr)},
		{"all digits accepted", "0x1111111111111111111111111111111111111111", "0x1111111111111111111111111111111111111111"},
	}

	for _, tc := range accept {
		t.Run(tc.name, func(t *testing.T) {
			// Given an address that is all-one-case or a valid EIP-55 mixed case
			// When canonicalized
			// Then the lowercase canonical form is returned
			got, err := CanonicalAddress(tc.raw)
			if err != nil {
				t.Fatalf("CanonicalAddress(%q) error = %v, want nil", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("CanonicalAddress(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}

	reject := []struct {
		name string
		raw  string
	}{
		{"mixed case failing EIP-55", badChecksumAddr},
		{"no 0x prefix", lowerAddr[2:]},
		{"too short", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAe"},
		{"too long", lowerAddr + "d"},
		{"non-hex character", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAeg"},
		{"empty", ""},
		{"uppercase X prefix", "0X" + upperAddr[2:]},
	}

	for _, tc := range reject {
		t.Run(tc.name, func(t *testing.T) {
			// Given an address failing shape or EIP-55 checksum
			// When canonicalized
			// Then it is rejected with field "recipient" and no value escapes
			got, err := CanonicalAddress(tc.raw)
			if got != "" {
				t.Errorf("value = %q, want empty", got)
			}
			requireValidationError(t, err, "recipient")
		})
	}
}

func TestValidateIdempotencyKeyAcceptsVerbatim(t *testing.T) {
	good := []struct {
		name string
		raw  string
	}{
		{"single visible char", "a"},
		{"bang", "!"},
		{"tilde", "~"},
		{"128 bytes", strings.Repeat("a", 128)},
		{"mixed case is not lowercased", "AbC-._~"},
	}

	for _, tc := range good {
		t.Run(tc.name, func(t *testing.T) {
			// Given an opaque key of 1-128 bytes in 0x21-0x7E
			// When validated
			// Then it is accepted verbatim (never trimmed, lowercased or converted)
			if err := ValidateIdempotencyKey(tc.raw); err != nil {
				t.Errorf("ValidateIdempotencyKey(%q) = %v, want nil", tc.raw, err)
			}
		})
	}
}

func TestValidateIdempotencyKeyRejectsBadShape(t *testing.T) {
	bad := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"129 bytes", strings.Repeat("a", 129)},
		{"space", "a b"},
		{"leading space no trim", " abc"},
		{"trailing space no trim", "abc "},
		{"tab", "a\tb"},
		{"newline", "a\nb"},
		{"carriage return", "a\rb"},
		{"DEL", "a\x7fb"},
		{"NUL", "a\x00b"},
		{"non-ASCII é", "café"},
		{"high byte", "a\x80b"},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			// Given a key that is missing, too long, or outside 0x21-0x7E
			// When validated
			// Then it is rejected with field "idempotency_key"
			requireValidationError(t, ValidateIdempotencyKey(tc.raw), "idempotency_key")
		})
	}
}

// TestValidateIdempotencyKeyNoConversion is a guard against a hidden
// normalization step: two keys differing only by case must both be valid and
// remain distinct inputs to the caller (the function returns the error only, so
// the observable contract is "both accepted, neither rewritten").
func TestValidateIdempotencyKeyNoConversion(t *testing.T) {
	if err := ValidateIdempotencyKey("abc"); err != nil {
		t.Errorf("abc = %v, want nil", err)
	}
	if err := ValidateIdempotencyKey("ABC"); err != nil {
		t.Errorf("ABC = %v, want nil", err)
	}
}

// TestAddressShapeMatchesConfigPrecedent confirms the validator agrees with the
// repository's existing address normalization for the shapes both accept.
func TestAddressShapeMatchesConfigPrecedent(t *testing.T) {
	if !common.IsHexAddress(lowerAddr) {
		t.Fatalf("test vector %q is not a hex address", lowerAddr)
	}
	got, err := CanonicalAddress(lowerAddr)
	if err != nil {
		t.Fatalf("CanonicalAddress(%q) error = %v", lowerAddr, err)
	}
	if got != strings.ToLower(common.HexToAddress(lowerAddr).Hex()) {
		t.Errorf("CanonicalAddress(%q) = %q, want config-style %q", lowerAddr, got,
			strings.ToLower(common.HexToAddress(lowerAddr).Hex()))
	}
}
