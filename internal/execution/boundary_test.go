// boundary_test.go executes the quickstart V12.3 boundary assertions (T048) as a
// static scan of this package's production (non-test) sources: no RPC/dial
// import and no upstream owner package in the business import graph, no
// non-SELECT statement against an upstream table, no signing/transaction/
// broadcast path, and exactly one 010 consumer boundary carrying identity only.
//
// The integration-tagged adapter seam (binding_live.go) is the single sanctioned
// exception: it may import 008's read provider so the read-only observation is
// real, but it must not reach any writer API. Everything else is closed.
package execution

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

type execSource struct {
	name        string
	src         string
	integration bool
}

// productionSources returns every non-test .go file in the package directory and
// flags the integration-tagged ones.
func productionSources(t *testing.T) []execSource {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []execSource
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		out = append(out, execSource{
			name:        name,
			src:         string(body),
			integration: strings.Contains(string(body), "//go:build integration"),
		})
	}
	if len(out) == 0 {
		t.Fatal("no production sources found")
	}
	return out
}

func parseSource(t *testing.T, s execSource) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), s.name, s.src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", s.name, err)
	}
	return f
}

func importPath(spec *ast.ImportSpec) string {
	return strings.Trim(spec.Path.Value, `"`)
}

// rpcPrefixes are RPC/dial or transaction-construction imports 011 must never
// carry (an ETF-free worker: no chain client, no signing, no broadcast).
var rpcPrefixes = []string{
	"net",
	"net/http",
	"net/rpc",
	"google.golang.org/grpc",
	"github.com/ethereum/go-ethereum",
}

// upstreamPrefixes are the 006/007/008/009/010 owner packages. 011's business
// graph imports none of them; the 008 read seam is the only integration-tagged
// exception.
var upstreamPrefixes = []string{
	"github.com/xtianxx/txharbor/internal/indexer",
	"github.com/xtianxx/txharbor/internal/withdrawal",
	"github.com/xtianxx/txharbor/internal/signer",
	"github.com/xtianxx/txharbor/internal/nonce",
	"github.com/xtianxx/txharbor/internal/txlifecycle",
	"github.com/xtianxx/txharbor/internal/app",
}

func hasPrefix(path string, prefixes []string) (string, bool) {
	for _, p := range prefixes {
		if path == p || strings.HasPrefix(path, p+"/") {
			return p, true
		}
	}
	return "", false
}

func TestV12BoundaryImports(t *testing.T) {
	for _, s := range productionSources(t) {
		file := parseSource(t, s)
		for _, spec := range file.Imports {
			path := importPath(spec)
			if p, bad := hasPrefix(path, rpcPrefixes); bad {
				t.Errorf("%s imports RPC/dial or tx-construction package %q", s.name, p)
			}
			if p, bad := hasPrefix(path, upstreamPrefixes); bad {
				if s.integration && p == "github.com/xtianxx/txharbor/internal/nonce" {
					continue // the single sanctioned 008 read-only seam
				}
				t.Errorf("%s imports upstream owner package %q", s.name, p)
			}
		}
		if s.integration && strings.Contains(s.src, "internal/nonce") {
			// The seam may only observe: no allocation/consumption/release.
			for _, verb := range []string{"Allocate", "Consume", "Release", "Reserve", "Bind("} {
				if strings.Contains(s.src, verb) {
					t.Errorf("%s uses an 008 writer/allocator verb %q across the read seam", s.name, verb)
				}
			}
		}
	}
}

// upstreamTables are the 006/007/008/009/010 tables 011 may read but never write.
var upstreamTables = []string{
	"withdrawal_requests", "withdrawal_authorizations", "withdrawal_authorization_scopes",
	"nonce_bindings", "nonce_wallet_registry", "nonce_scope_state", "nonce_scope_holds",
	"tx_attempts", "signing_requests",
	"indexer_pause", "log_pause", "deposit_pause",
	"reorg_recovery", "reorg_recovery_events", "chain_blocks",
}

var writeKeywords = []string{"INSERT ", "UPDATE ", "DELETE ", "ALTER ", "DROP ", "TRUNCATE ", "CREATE "}

// sqlVerbs marks a literal as a statement rather than a bare table-name list.
var sqlVerbs = []string{"SELECT", "INSERT", "UPDATE", "DELETE", "LOCK", "ALTER", "DROP", "TRUNCATE", "CREATE"}

func TestV12BoundaryNoUpstreamWrites(t *testing.T) {
	for _, s := range productionSources(t) {
		file := parseSource(t, s)
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			sql, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			upper := strings.ToUpper(sql)
			var touched string
			for _, table := range upstreamTables {
				if strings.Contains(sql, table) {
					touched = table
					break
				}
			}
			if touched == "" {
				return true
			}
			if !containsAny(upper, sqlVerbs) {
				return true // a bare table-name list, not a statement
			}
			trimmed := strings.TrimSpace(upper)
			isSelect := strings.HasPrefix(trimmed, "SELECT") || strings.HasPrefix(trimmed, "WITH")
			// The gate protocol locks upstream tables IN SHARE MODE before the
			// read sequence (gates.md §0/J3): a shared lock, never a write.
			isShareLock := strings.HasPrefix(trimmed, "LOCK TABLE") && strings.Contains(trimmed, "SHARE MODE")
			if !isSelect && !isShareLock {
				t.Errorf("%s: literal touching upstream table %q is not a SELECT: %q", s.name, touched, sql)
			}
			for _, kw := range writeKeywords {
				if strings.Contains(upper, kw) {
					t.Errorf("%s: non-SELECT statement against upstream table %q: %q", s.name, touched, sql)
				}
			}
			return true
		})
	}
}

func containsAny(s string, tokens []string) bool {
	for _, t := range tokens {
		if strings.Contains(s, t) {
			return true
		}
	}
	return false
}

func TestV12BoundaryNoSigningOrBroadcastPath(t *testing.T) {
	forbidden := []string{
		"SignTx", "SignHash", "SignMessage", "ecdsa", "rlp.", "types.NewTx",
		"types.Transaction", "SendTransaction", "SendRawTransaction",
		"eth_sendRawTransaction", "RawTransaction", "signedTx",
	}
	for _, s := range productionSources(t) {
		for _, token := range forbidden {
			if strings.Contains(s.src, token) {
				t.Errorf("%s exposes a signing/transaction/broadcast path (%q)", s.name, token)
			}
		}
	}
}

// TestV12BoundarySingleConsumer checks the 010 boundary is exactly one interface
// pair carrying identity/fencing/action only — no nonce, calldata, signatures or
// raw bytes — and that the business graph never imports 010's package. The
// round-2 fee-replacement contract is the single sanctioned exception to the
// identity-only rule: 010 has no fee oracle, so ActionReplace carries the
// caller-supplied replacement candidate (fee dimensions plus the replacement's
// grant/signing identity) and 010 validates it. The exception is closed to
// exactly those four fields; every other field and struct stays identity-only.
func TestV12BoundarySingleConsumer(t *testing.T) {
	advancer, reader := 0, 0
	for _, s := range productionSources(t) {
		file := parseSource(t, s)
		for _, decl := range file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				if _, ok := ts.Type.(*ast.InterfaceType); !ok {
					continue
				}
				switch ts.Name.Name {
				case "LifecycleAdvancer":
					advancer++
				case "LifecycleReader":
					reader++
				}
			}
		}
	}
	if advancer != 1 || reader != 1 {
		t.Fatalf("consumer boundary interfaces: LifecycleAdvancer=%d LifecycleReader=%d, want exactly one each", advancer, reader)
	}

	// The boundary payload must not smuggle non-identity values into 011's
	// request (construction belongs to 010, signing to 009). The four
	// sanctioned replacement-candidate fields are the only exception; any new
	// fee/gas/signing-shaped field still fails this scan.
	sanctioned := map[string]bool{
		"ReplacementFeeMaxPerGas":            true,
		"ReplacementFeeMaxPriorityFeePerGas": true,
		"ReplacementAuthorizationID":         true,
		"ReplacementSigningRequestID":        true,
	}
	sanctionedSeen := map[string]bool{}
	boundaryStructs := map[string]bool{
		"AdvanceRequest": true, "AdvanceOutcome": true,
		"AttemptRef": true, "UnknownRef": true, "LifecycleFacts": true,
	}
	forbiddenField := []string{"fee", "gas", "nonce", "calldata", "signature", "sign", "raw", "rlp", "txbytes", "signed"}
	seen := map[string]bool{}
	for _, s := range productionSources(t) {
		file := parseSource(t, s)
		ast.Inspect(file, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || !boundaryStructs[ts.Name.Name] {
				return true
			}
			seen[ts.Name.Name] = true
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				for _, name := range field.Names {
					if ts.Name.Name == "AdvanceRequest" && sanctioned[name.Name] {
						sanctionedSeen[name.Name] = true
						continue
					}
					lower := strings.ToLower(name.Name)
					for _, bad := range forbiddenField {
						if strings.Contains(lower, bad) {
							t.Errorf("%s field %s.%s smuggles a non-identity value across the boundary", s.name, ts.Name.Name, name.Name)
						}
					}
				}
			}
			return true
		})
	}
	for name := range sanctioned {
		if !sanctionedSeen[name] {
			t.Errorf("sanctioned replacement field AdvanceRequest.%s is missing", name)
		}
	}
	for name := range boundaryStructs {
		if !seen[name] {
			t.Errorf("boundary struct %s not found", name)
		}
	}
}
