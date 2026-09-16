package nonce

import (
	"go/ast"
	"go/parser"
	"go/token"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func mustBig(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("bad test literal %q", s)
	}
	return n
}

// TestNumeric pins the R13 integer-only bounds: 0 and 2⁶⁴−1 are accepted;
// 2⁶⁴, negative values and non-decimal input are refused. No float64 exists
// on any path.
func TestNumeric(t *testing.T) {
	t.Run("decimal bounds", func(t *testing.T) {
		for _, s := range []string{"0", "1", "18446744073709551615"} {
			n, err := ParseDecimal(s)
			if err != nil {
				t.Fatalf("ParseDecimal(%q) error = %v", s, err)
			}
			if n.String() != s {
				t.Fatalf("ParseDecimal(%q) = %s", s, n.String())
			}
		}
		for _, s := range []string{
			"18446744073709551616", // 2⁶⁴
			"-1",
			"+1",
			" 1",
			"1 ",
			"",
			"0x10",
			"1.0",
			"abc",
			"１２３", // non-ASCII digits
		} {
			if _, err := ParseDecimal(s); err == nil {
				t.Fatalf("ParseDecimal(%q): expected refusal", s)
			}
		}
	})

	t.Run("hex quantities", func(t *testing.T) {
		cases := map[string]string{
			"0x0":                "0",
			"0xf":                "15",
			"0xff":               "255",
			"0xffffffffffffffff": "18446744073709551615",
			"0x0000000000000001": "1",
		}
		for in, want := range cases {
			n, err := ParseHexQuantity(in)
			if err != nil {
				t.Fatalf("ParseHexQuantity(%q) error = %v", in, err)
			}
			if n.String() != want {
				t.Fatalf("ParseHexQuantity(%q) = %s, want %s", in, n.String(), want)
			}
		}
		// 2⁶⁴ and beyond overflow-refuse explicitly.
		for _, in := range []string{"0x10000000000000000", "0x1" + strings.Repeat("0", 20), "0x", "0X1", "1", "0xzz"} {
			if _, err := ParseHexQuantity(in); err == nil {
				t.Fatalf("ParseHexQuantity(%q): expected refusal", in)
			}
		}
	})

	t.Run("range and overflow", func(t *testing.T) {
		if err := ValidateNonceRange(mustBig(t, "0")); err != nil {
			t.Fatalf("0 refused: %v", err)
		}
		if err := ValidateNonceRange(mustBig(t, MaxNonceDecimal)); err != nil {
			t.Fatalf("2^64-1 refused: %v", err)
		}
		if err := ValidateNonceRange(mustBig(t, "18446744073709551616")); err == nil {
			t.Fatal("2^64 accepted")
		}
		if err := ValidateNonceRange(mustBig(t, "-1")); err == nil {
			t.Fatal("negative accepted")
		}
		if err := ValidateNonceRange(nil); err == nil {
			t.Fatal("nil accepted")
		}

		next, err := NextNonce(mustBig(t, "0"))
		if err != nil || next.String() != "1" {
			t.Fatalf("NextNonce(0) = %v, %v", next, err)
		}
		next, err = NextNonce(mustBig(t, "18446744073709551614"))
		if err != nil || next.String() != MaxNonceDecimal {
			t.Fatalf("NextNonce(2^64-2) = %v, %v", next, err)
		}
		if _, err := NextNonce(MaxNonceBig()); err == nil {
			t.Fatal("NextNonce(2^64-1) did not overflow-refuse")
		}
	})

	t.Run("pgtype roundtrip", func(t *testing.T) {
		for _, s := range []string{"0", "7", MaxNonceDecimal} {
			v := NumericValue(mustBig(t, s))
			if !v.Valid || v.Exp != 0 {
				t.Fatalf("NumericValue(%s) = %+v", s, v)
			}
			back, err := BigFromNumeric(v)
			if err != nil || back.String() != s {
				t.Fatalf("BigFromNumeric roundtrip %s = %v, %v", s, back, err)
			}
		}
		if v, err := BigFromNumeric(pgtype.Numeric{}); v != nil || err != nil {
			t.Fatalf("BigFromNumeric(NULL) = %v, %v", v, err)
		}
		// A scale-0 value decoded by pgx can carry a positive exponent
		// (trailing-zero reduction): 10 with Int=1, Exp=1 must read back 10.
		v10, err := BigFromNumeric(pgtype.Numeric{Int: big.NewInt(1), Exp: 1, Valid: true})
		if err != nil || v10.String() != "10" {
			t.Fatalf("BigFromNumeric(1e1) = %v, %v", v10, err)
		}
		if _, err := BigFromNumeric(pgtype.Numeric{Int: big.NewInt(1), Exp: -1, Valid: true}); err == nil {
			t.Fatal("fractional numeric accepted")
		}
	})

	t.Run("format", func(t *testing.T) {
		if got := FormatDecimal(nil); got != "" {
			t.Fatalf("FormatDecimal(nil) = %q", got)
		}
		if got := FormatHexQuantity(mustBig(t, "255")); got != "0xff" {
			t.Fatalf("FormatHexQuantity(255) = %q", got)
		}
		if got := FormatDecimal(big.NewInt(0)); got != "0" {
			t.Fatalf("FormatDecimal(0) = %q", got)
		}
		if got := MaxBig(nil, big.NewInt(3)).String(); got != "3" {
			t.Fatalf("MaxBig(nil,3) = %s", got)
		}
		if got := MaxBig(big.NewInt(3), nil).String(); got != "3" {
			t.Fatalf("MaxBig(3,nil) = %s", got)
		}
		if got := MaxBig(big.NewInt(3), big.NewInt(4)).String(); got != "4" {
			t.Fatalf("MaxBig(3,4) = %s", got)
		}
		if MaxBig(nil, nil) != nil {
			t.Fatal("MaxBig(nil,nil) != nil")
		}
	})

	// R13 drift guard: AST scan, so doc comments naming the banned types (as
	// numeric.go's own does) don't trip it.
	t.Run("zero float64", func(t *testing.T) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "numeric.go", nil, 0)
		if err != nil {
			t.Fatalf("parse numeric.go: %v", err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if ok && (id.Name == "float64" || id.Name == "float32") {
				t.Fatalf("numeric.go uses %s at %s", id.Name, fset.Position(id.Pos()))
			}
			return true
		})
	})
}
