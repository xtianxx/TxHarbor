// numeric.go owns the integer-only numeric representation of 008 (R13, 原则
// I): nonce/count values are NUMERIC(78,0) in PostgreSQL and math/big in Go.
// JSON-RPC hex quantities are parsed with explicit overflow refusal; there is
// zero float64 anywhere in this package. The EVM account-nonce range is
// 0 ≤ n ≤ 2⁶⁴−1.
package nonce

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// MaxNonceDecimal is the decimal form of 2⁶⁴−1, the EVM account-nonce upper
// bound declared by every 008 NUMERIC(78,0) CHECK.
const MaxNonceDecimal = "18446744073709551615"

// MaxNonceBig returns a fresh 2⁶⁴−1 big.Int (never shared: callers may mutate
// the returned value).
func MaxNonceBig() *big.Int {
	return new(big.Int).SetUint64(math.MaxUint64)
}

// ErrOutOfRange is the range refusal shared by the parse helpers.
var ErrOutOfRange = errors.New("value is outside the nonce range 0..2^64-1")

// ParseDecimal parses a canonical unsigned decimal integer and enforces the
// 0 ≤ n ≤ 2⁶⁴−1 range. Sign characters, whitespace, empty strings and
// non-digits are refused (a negative value is refused, never wrapped).
func ParseDecimal(s string) (*big.Int, error) {
	if s == "" {
		return nil, errors.New("empty decimal value")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return nil, fmt.Errorf("%q is not an unsigned decimal integer", s)
		}
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("%q is not an unsigned decimal integer", s)
	}
	if err := ValidateNonceRange(n); err != nil {
		return nil, err
	}
	return n, nil
}

// ParseHexQuantity parses a JSON-RPC hex quantity ("0x0", "0x1f", …) and
// enforces the 0 ≤ n ≤ 2⁶⁴−1 range with explicit overflow refusal. The empty
// quantity "0x" and non-hex digits are refused; leading zeros beyond the
// canonical form are tolerated (PostgreSQL and go-ethereum both accept them).
func ParseHexQuantity(s string) (*big.Int, error) {
	if !strings.HasPrefix(s, "0x") {
		return nil, fmt.Errorf("%q is not a 0x-prefixed hex quantity", s)
	}
	digits := s[2:]
	if digits == "" {
		return nil, fmt.Errorf("%q is an empty hex quantity", s)
	}
	for i := 0; i < len(digits); i++ {
		c := digits[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return nil, fmt.Errorf("%q is not a hex quantity", s)
		}
	}
	n, ok := new(big.Int).SetString(digits, 16)
	if !ok {
		return nil, fmt.Errorf("%q is not a hex quantity", s)
	}
	if err := ValidateNonceRange(n); err != nil {
		return nil, err
	}
	return n, nil
}

// ValidateNonceRange refuses values outside 0 ≤ n ≤ 2⁶⁴−1. nil is refused
// (missing values are never treated as zero).
func ValidateNonceRange(n *big.Int) error {
	if n == nil {
		return errors.New("missing numeric value")
	}
	if n.Sign() < 0 || n.Cmp(MaxNonceBig()) > 0 {
		return fmt.Errorf("%s is outside the nonce range 0..%s", n.String(), MaxNonceDecimal)
	}
	return nil
}

// NextNonce returns n+1 with explicit overflow refusal: the value after
// 2⁶⁴−1 does not exist in the EVM nonce space, so allocation refuses instead
// of wrapping or saturating.
func NextNonce(n *big.Int) (*big.Int, error) {
	if err := ValidateNonceRange(n); err != nil {
		return nil, err
	}
	next := new(big.Int).Add(n, big.NewInt(1))
	if next.Cmp(MaxNonceBig()) > 0 {
		return nil, fmt.Errorf("nonce space exhausted at %s", MaxNonceDecimal)
	}
	return next, nil
}

// MaxBig returns the larger of a and b (nil sorts as -∞ so MaxBig(nil, x) is
// x). The result is a fresh big.Int.
func MaxBig(a, b *big.Int) *big.Int {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return new(big.Int).Set(b)
	case b == nil:
		return new(big.Int).Set(a)
	case a.Cmp(b) >= 0:
		return new(big.Int).Set(a)
	default:
		return new(big.Int).Set(b)
	}
}

// FormatDecimal renders a value in canonical decimal (no sign, no
// separators); nil renders as the empty string.
func FormatDecimal(n *big.Int) string {
	if n == nil {
		return ""
	}
	return n.String()
}

// FormatHexQuantity renders a value as a JSON-RPC hex quantity ("0x0", …).
func FormatHexQuantity(n *big.Int) string {
	if n == nil {
		return ""
	}
	return "0x" + n.Text(16)
}

// NumericValue converts a big.Int into the pgtype.Numeric carrier for the
// NUMERIC(78,0) columns (scale 0). nil becomes SQL NULL.
func NumericValue(n *big.Int) pgtype.Numeric {
	if n == nil {
		return pgtype.Numeric{}
	}
	return pgtype.Numeric{Int: new(big.Int).Set(n), Exp: 0, Valid: true}
}

// BigFromNumeric converts a scanned NUMERIC(78,0) back to a big.Int. A SQL
// NULL scans to (nil, nil); a non-finite or non-integral value is refused
// (the columns cannot hold one, so this is a corruption tripwire).
func BigFromNumeric(n pgtype.Numeric) (*big.Int, error) {
	if !n.Valid {
		return nil, nil
	}
	if n.NaN || n.InfinityModifier != pgtype.Finite {
		return nil, errors.New("numeric value is not finite")
	}
	v := new(big.Int)
	if n.Int != nil {
		v.Set(n.Int)
	}
	switch {
	case n.Exp > 0:
		v.Mul(v, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil))
	case n.Exp < 0:
		div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n.Exp)), nil)
		q, r := new(big.Int).QuoRem(v, div, new(big.Int))
		if r.Sign() != 0 {
			return nil, fmt.Errorf("numeric value has a fractional part (exp=%d)", n.Exp)
		}
		v = q
	}
	return v, nil
}
