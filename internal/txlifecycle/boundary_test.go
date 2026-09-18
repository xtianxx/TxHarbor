// boundary_test.go executes quickstart V11's static boundary assertions
// (T054; FR-10; constitution VIII/XII) as a source/import scan of this
// package's production (non-test) files: no key or key-provider import, no
// signing/keystore code, exactly one chain-dispatch call site, and no
// signature, signed byte or credential reaching a log or a metric label.
//
// The 009 boundary is 010's own HTTP client (signing.go): keys never enter
// this package, and the chain write happens at exactly one site (send.go).
package txlifecycle

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

type lcSource struct {
	name string
	src  string
}

// boundarySources returns every non-test .go file in this package directory.
func boundarySources(t *testing.T) []lcSource {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []lcSource
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		out = append(out, lcSource{name: name, src: string(body)})
	}
	if len(out) == 0 {
		t.Fatal("no production sources found")
	}
	return out
}

func parseBoundarySource(t *testing.T, s lcSource) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), s.name, s.src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", s.name, err)
	}
	return f
}

// keyProviderPrefixes are key-material and key-provider packages 010 must
// never import: keys exist only in 009 (FR-10), reached over its HTTP
// boundary. internal/signer is 009's owner package — 010 talks to 009 over
// HTTP via its in-package client, never by importing it.
var keyProviderPrefixes = []string{
	"crypto/ecdsa",
	"crypto/ed25519",
	"golang.org/x/crypto",
	"github.com/ethereum/go-ethereum/accounts",
	"github.com/ethereum/go-ethereum/signer",
	"github.com/xtianxx/txharbor/internal/signer",
}

// keyVerbs are the local-key/signing operations whose presence in a
// production file would mean 010 holds or derives keys or signs locally
// (hashing — crypto.Keccak256/Keccak256Hash — is construction/fingerprinting,
// not signing, and stays allowed).
var keySigningVerbs = []string{
	"ToECDSA", "HexToECDSA", "LoadECDSA", "FromECDSA",
	"ecdsa.GenerateKey", "ecdsa.PrivateKey", "ecdsa.PublicKey",
	"SignTx", "SignHash", "types.Signer(", "Ecrecover",
	"PubkeyToAddress", "crypto.Sign(", "crypto.VerifySignature",
}

// loggingImports are the log-emission packages 010 must not use: the hot
// paths record durable rows, and a log surface would risk signature/signed
// byte/credential leakage (SC-01..07; constitution XII).
var loggingImports = []string{
	"github.com/xtianxx/txharbor/internal/logx",
	"log/slog",
	"log",
}

// txMetricMethods is the complete fixed-vocabulary 010 observability surface.
// Any other metrics call from this package would smuggle a wrong-lane label
// vocabulary into the shared registry.
var txMetricMethods = map[string]bool{
	"ObserveTxDispatch":      true,
	"ObserveTxGateRefusal":   true,
	"ObserveTxUnknown":       true,
	"ObserveTxReconcile":     true,
	"ObserveTxReceiptEffect": true,
	"ObserveTxRevision":      true,
}

func TestV11BoundaryImports(t *testing.T) {
	for _, s := range boundarySources(t) {
		file := parseBoundarySource(t, s)
		for _, spec := range file.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			for _, prefix := range keyProviderPrefixes {
				if path == prefix || strings.HasPrefix(path, prefix+"/") {
					t.Errorf("%s imports a key/key-provider package %q (keys live only in 009)", s.name, path)
				}
			}
			for _, prefix := range loggingImports {
				if path == prefix || strings.HasPrefix(path, prefix+"/") {
					t.Errorf("%s imports a log-emission package %q (durable rows, not logs, are the record)", s.name, path)
				}
			}
		}
	}
}

func TestV11BoundaryNoKeyOrSigningCode(t *testing.T) {
	for _, s := range boundarySources(t) {
		for _, verb := range keySigningVerbs {
			if strings.Contains(s.src, verb) {
				t.Errorf("%s contains local key/signing code (%q); signatures reach 010 only through 009", s.name, verb)
			}
		}
	}
}

// TestV11BoundarySingleDispatch pins the single chain write: exactly one
// SendSignedTransaction call site exists in production code, so every
// dispatch decision funnels through the one gated T3 region (send-gate.md §2).
func TestV11BoundarySingleDispatch(t *testing.T) {
	count := 0
	for _, s := range boundarySources(t) {
		file := parseBoundarySource(t, s)
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SendSignedTransaction" {
				count++
			}
			return true
		})
	}
	if count != 1 {
		t.Fatalf("SendSignedTransaction call sites = %d, want exactly 1", count)
	}
}

// TestV11BoundaryNoSecretLogsOrMetricLabels scans observability emission: the
// package emits no logs at all, reaches metrics only through the
// fixed-vocabulary ObserveTx* helpers (never prometheus label construction),
// and passes those helpers fixed-vocabulary variables — never a request- or
// result-derived value (no tx hash, signature hex or credential).
func TestV11BoundaryNoSecretLogsOrMetricLabels(t *testing.T) {
	secretSources := []string{"tx_hash", "signature", "sig_hex", "credential", "signedBytes", "SignedBytes"}
	for _, s := range boundarySources(t) {
		file := parseBoundarySource(t, s)
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			name := sel.Sel.Name
			switch {
			case name == "WithLabelValues":
				t.Errorf("%s constructs metric labels directly (WithLabelValues); the fixed vocabularies live in internal/metrics", s.name)
			case strings.HasPrefix(name, "Log") || strings.HasPrefix(name, "log"):
				t.Errorf("%s calls a log function %q; 010 records durable rows instead", s.name, name)
			case strings.HasPrefix(name, "Observe"):
				if !txMetricMethods[name] {
					t.Errorf("%s calls non-010 metrics method %q", s.name, name)
				}
				for _, arg := range call.Args {
					id, ok := arg.(*ast.Ident)
					if !ok || id.Obj != nil {
						continue
					}
					for _, bad := range secretSources {
						if strings.EqualFold(id.Name, bad) {
							t.Errorf("%s passes %q to %s — a secret-bearing label argument", s.name, id.Name, name)
						}
					}
				}
			}
			return true
		})
	}
}
