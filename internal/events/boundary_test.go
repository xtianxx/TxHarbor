// boundary_test.go executes the T016 import/payload boundary assertions as a
// static scan of this package's production (non-test) sources plus runtime
// payload checks. internal/events MUST NOT import upstream writer packages
// (indexer/withdrawal/execution/txlifecycle/nonce) or RPC/dial/signer
// packages: upstream business transactions call Append, so the dependency
// arrow is upstream -> events only. There are no triggers, no CDC and no
// network publish call anywhere in this package; payloads and log material
// never carry keys, credentials or raw signature bytes (FR-02/03; research
// R18; plan Structure Decision; constitution VIII).
package events

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

type eventSource struct {
	name string
	src  string
}

// productionSources returns every non-test .go file in the package directory.
// Test files are excluded: they legitimately import test scaffolding (the
// integration test imports internal/db for the real migration), while the
// production boundary is what ships.
func productionSources(t *testing.T) []eventSource {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []eventSource
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		out = append(out, eventSource{name: name, src: string(body)})
	}
	if len(out) == 0 {
		t.Fatal("no production sources found")
	}
	return out
}

func parseEventSource(t *testing.T, file eventSource) *ast.File {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file.name, file.src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file.name, err)
	}
	return parsed
}

// forbiddenImportPrefixes are RPC/dial, signer/key-material and upstream
// writer packages this package must never import.
var forbiddenImportPrefixes = []string{
	// RPC/dial and chain-client packages.
	"net",
	"google.golang.org/grpc",
	"github.com/ethereum/go-ethereum",
	"github.com/xtianxx/txharbor/internal/eth",
	// Signing / key material.
	"github.com/xtianxx/txharbor/internal/signer",
	// Upstream business writers (import-cycle prevention and funding-gate
	// isolation: no gate may ever be read from this package).
	"github.com/xtianxx/txharbor/internal/indexer",
	"github.com/xtianxx/txharbor/internal/withdrawal",
	"github.com/xtianxx/txharbor/internal/execution",
	"github.com/xtianxx/txharbor/internal/txlifecycle",
	"github.com/xtianxx/txharbor/internal/nonce",
	"github.com/xtianxx/txharbor/internal/app",
}

func TestT016ImportBoundary(t *testing.T) {
	for _, file := range productionSources(t) {
		parsed := parseEventSource(t, file)
		for _, spec := range parsed.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			for _, prefix := range forbiddenImportPrefixes {
				if path == prefix || strings.HasPrefix(path, prefix+"/") {
					t.Errorf("%s imports forbidden package %q", file.name, path)
				}
			}
		}
	}
}

// forbiddenRuntimeTokens are trigger/CDC paths this package must never carry
// (R1; ADR-013 §5). The outbox is written by explicit Append calls only.
var forbiddenRuntimeTokens = []string{
	"CREATE TRIGGER",
	"CREATE OR REPLACE FUNCTION",
	"debezium",
	"Debezium",
	"pglogrepl",
	"wal2json",
	"pgoutput",
	"logical_replication",
}

// forbiddenPublishTokens are broker/network publish calls. They must never
// appear in the transaction-bound emission path: publishing happens in the
// publisher runtime outside any transaction, never inside this package's
// Append path (T016 "事务内无网络发布调用"; contracts/outbox-publisher.md §0).
// The publisher runtime itself is the single place allowed to carry the broker
// client (T033); its "publish outside any transaction / never publish an
// uncommitted row" property is asserted at runtime by T036.
var forbiddenPublishTokens = []string{
	"ProduceSync",
	"franz-go",
	"twmb/franz",
	"kgo.",
	"sarama",
	"SendMessage",
	"net.Dial",
	"http.Post",
	"grpc.Dial",
}

// publisherRuntimeSources are the files allowed to carry a broker client. The
// list is explicit so a publish call added anywhere else still fails this test.
var publisherRuntimeSources = map[string]bool{
	"publisher.go": true,
}

func TestT016NoTriggerCDCOrNetworkPublish(t *testing.T) {
	for _, file := range productionSources(t) {
		for _, token := range forbiddenRuntimeTokens {
			if strings.Contains(file.src, token) {
				t.Errorf("%s carries a trigger/CDC path (%q)", file.name, token)
			}
		}
		if publisherRuntimeSources[file.name] {
			continue
		}
		for _, token := range forbiddenPublishTokens {
			if strings.Contains(file.src, token) {
				t.Errorf("%s carries a network publish call (%q)", file.name, token)
			}
		}
	}
}

// credentialPatterns detect literal key/credential material in sources
// (payload/log scan; data-model §9). They are deliberately literal-shaped
// (PEM markers, 32-byte hex, assigned secrets, provider key prefixes) so the
// scanner's own forbidden-token vocabulary does not trip them.
var credentialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]+-----`),
	regexp.MustCompile(`\b0x[0-9a-fA-F]{64}\b`),
	regexp.MustCompile(`(?i)(private[_-]?key|mnemonic|seed[_-]?phrase|api[_-]?key|password)\s*[:=]\s*["'][^"']+["']`),
	regexp.MustCompile(`(?i)\b(sk_live_|AKIA[0-9A-Z]{16})`),
}

func TestT016NoCredentialLiterals(t *testing.T) {
	for _, file := range productionSources(t) {
		for _, pattern := range credentialPatterns {
			if match := pattern.FindString(file.src); match != "" {
				t.Errorf("%s carries credential-shaped literal material (%q)", file.name, match)
			}
		}
	}
}

// TestT016AppendRequiresTransaction pins the only emission shape: the exported
// Append function takes an in-progress pgx.Tx. There is no pool-taking or
// post-commit variant that could emit outside the business transaction
// (FR-07; data-model §5 T1).
func TestT016AppendRequiresTransaction(t *testing.T) {
	found := false
	for _, file := range productionSources(t) {
		parsed := parseEventSource(t, file)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name.Name != "Append" {
				continue
			}
			found = true
			params := fn.Type.Params.List
			if len(params) != 3 {
				t.Fatalf("Append has %d parameters, want (ctx, tx, ev)", len(params))
			}
			selector, ok := params[1].Type.(*ast.SelectorExpr)
			if !ok {
				t.Fatalf("Append parameter 2 is %T, want pgx.Tx", params[1].Type)
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok || pkg.Name != "pgx" || selector.Sel.Name != "Tx" {
				t.Fatalf("Append parameter 2 = %s, want pgx.Tx", selector.Sel.Name)
			}
		}
	}
	if !found {
		t.Fatal("no package-level Append function found")
	}
}

// TestT016PayloadRuntimeScan is the runtime half of the payload scan: the
// catalog validator rejects forbidden keys and PEM material while allowing
// chain identity and revision fields (data-model §9).
func TestT016PayloadRuntimeScan(t *testing.T) {
	forbidden := []map[string]any{
		{"private_key": "x"},
		{"blob": "-----BEGIN PRIVATE KEY-----"},
		{"nested": []any{map[string]any{"api_key": "x"}}},
	}
	for _, payload := range forbidden {
		if key, found := ForbiddenPayload(payload); !found {
			t.Fatalf("payload %v passed the runtime scan", payload)
		} else if key == "" {
			t.Fatalf("payload %v reported an empty offending key", payload)
		}
	}
	allowed := []map[string]any{
		{"block_hash": "0xabc", "tx_hash": "0xdef", "log_index": 0},
		{"superseded_identity": map[string]any{"event_id": "x"}, "recovery_version": 1},
	}
	for _, payload := range allowed {
		if key, found := ForbiddenPayload(payload); found {
			t.Fatalf("allowed payload %v rejected on %q", payload, key)
		}
	}
}
