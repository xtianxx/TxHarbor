package txlifecycle

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forbiddenImports would put key material or a signing provider inside 010
// (FR-10; R-010-01). go-ethereum/crypto is allowed: this package uses only its
// hashing/address helpers, never key loading or signing.
var forbiddenImports = []string{
	"crypto/ecdsa",
	"crypto/ed25519",
	"crypto/elliptic",
	"github.com/ethereum/go-ethereum/accounts",
	"github.com/ethereum/go-ethereum/accounts/keystore",
	"github.com/xtianxx/txharbor/internal/signer",
}

// TestV11BoundaryStatic is T054/V11: the package imports no key/provider
// package and has exactly one dispatch call site (FR-10; plan §boundary).
func TestV11BoundaryStatic(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	dispatchSites := 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			for _, bad := range forbiddenImports {
				if p == bad || strings.HasPrefix(p, bad+"/") {
					t.Errorf("%s imports forbidden key/provider package %q", path, p)
				}
			}
		}
		dispatchSites += strings.Count(string(src), ".SendSignedTransaction(")
	}
	if dispatchSites != 1 {
		t.Errorf("dispatch call sites = %d, want exactly 1", dispatchSites)
	}
}

// TestV11NoSecretsInLogsOrMetrics is T054/V11's secrecy scan: 010 does not log,
// and no metric call receives a signature, signed byte or credential token
// (FR-10; constitution VIII/XII; R-010-13).
func TestV11NoSecretsInLogsOrMetrics(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	secretTokens := []string{"Signature", "signedBytes", "SignedBytes", "signed_tx_bytes", "Credential", "credential", "Bearer"}
	logCalls := []string{"logx.", "slog.", "log.Print", "log.Printf", "log.Println"}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			for _, lc := range logCalls {
				if strings.Contains(line, lc) {
					t.Errorf("%s:%d uses logging (%s); 010 holds no secret to log (FR-10)", path, i+1, lc)
				}
			}
			if strings.Contains(line, ".Observe") {
				for _, tok := range secretTokens {
					if strings.Contains(line, tok) {
						t.Errorf("%s:%d passes %q to a metric call", path, i+1, tok)
					}
				}
			}
		}
	}
}
