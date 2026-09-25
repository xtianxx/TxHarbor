// layering_audit_test.go implements the T085 layering-independence audit as a
// Unit-layer static scan (no middleware, no Docker):
//
//  1. Unit-eligible test files (no layer build tag) never import
//     testcontainers or internal/testutil, so `make test` cannot depend on an
//     external middleware daemon.
//  2. Every layer — Integration-PG / Redis / Kafka, Contract, E2E, Fault,
//     Perf — has at least one tagged test file (no vacuous layer) and a
//     Makefile entry point; the 013-added layer targets carry the
//     require_tagged_tests guard so an empty layer reports NOT RUN instead of
//     passing silently (verification.md §4). The two 013 verification-
//     supplement harness tags (integration_backlog, integration_dualproc) are
//     non-unit layers under the same non-vacuity check, but stay deliberately
//     opt-in: no Makefile target and no CI invocation (a 5k–10k-event drill
//     and a ~10-minute dual-process run are not ordinary-PR gates).
//  3. Redis-tagged files never import the Kafka client and Kafka-tagged files
//     never import the Redis client: the component layers stay independently
//     runnable.
//  4. Fault/Perf never run on ordinary PRs (ci.yml carries no test-fault or
//     test-perf invocation; fault-perf.yml is scheduled/manual/callable only)
//     and never enter the production build (no non-test import of
//     internal/faultdrill or internal/perf).
//  5. go.mod, compose.yaml and the CI workflows carry zero Debezium/CDC/
//     Kubernetes runtime or test dependencies (ADR-013 §5; FR-28).
//  6. internal/events keeps its import boundary: no RPC, signer, cache,
//     rate-limit or upstream writer imports; the broker client (franz-go)
//     appears only in the publisher and consumer runtime files.
//
// Violations fail the test (FR-03/28; adr.md §5; constitution XIII).
package app

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// auditLayerTags are the build tags that select a non-unit test layer. The
// 013 verification-supplement harness tags (integration_backlog,
// integration_dualproc) are registered here so their Docker-backed files are
// never misread as unit-eligible; they stay opt-in (no Makefile or CI target).
var auditLayerTags = []string{
	"integration",
	"integration_redis",
	"integration_kafka",
	"contract",
	"e2e",
	"fault",
	"perf",
	"integration_backlog",
	"integration_dualproc",
}

func auditRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root (go.mod) not found above the test directory")
		}
		dir = parent
	}
}

// auditGoFiles returns every .go file under internal/ and cmd/ (sorted).
func auditGoFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, dir := range []string{"internal", "cmd"} {
		base := filepath.Join(root, dir)
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				switch entry.Name() {
				case ".git", ".evidence", ".omo", ".slim", "node_modules":
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(path, ".go") {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", base, err)
		}
	}
	sort.Strings(out)
	return out
}

var (
	auditBuildLine = regexp.MustCompile(`(?m)^//go:build\s+(.+)$`)
	auditTagIdent  = regexp.MustCompile(`[a-z_][a-z0-9_]*`)
)

// auditBuildTags extracts the identifiers of a //go:build constraint.
func auditBuildTags(t *testing.T, path string) []string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	match := auditBuildLine.FindSubmatch(body)
	if match == nil {
		return nil
	}
	return auditTagIdent.FindAllString(string(match[1]), -1)
}

func auditHasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

// auditImports parses only the import block of a Go file.
func auditImports(t *testing.T, path string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports %s: %v", path, err)
	}
	var out []string
	for _, spec := range parsed.Imports {
		out = append(out, strings.Trim(spec.Path.Value, `"`))
	}
	return out
}

func auditImportHasPrefix(imports []string, prefix string) bool {
	for _, path := range imports {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

// auditMakefileTarget returns the recipe lines of one Makefile target.
func auditMakefileTarget(t *testing.T, makefile, name string) string {
	t.Helper()
	var recipe strings.Builder
	inTarget := false
	for _, line := range strings.Split(makefile, "\n") {
		if strings.HasPrefix(line, name+":") {
			inTarget = true
			continue
		}
		if !inTarget {
			continue
		}
		if strings.HasPrefix(line, "\t") {
			recipe.WriteString(line)
			recipe.WriteString("\n")
			continue
		}
		break
	}
	if recipe.Len() == 0 {
		t.Fatalf("Makefile target %q not found or has an empty recipe", name)
	}
	return recipe.String()
}

// TestT085UnitLayerNeedsNoMiddleware asserts that no unit-eligible test file
// (one without a layer build tag) pulls in the Docker-backed testutil helpers,
// and that a unit-eligible file importing testcontainers guards the container
// case with a skip helper (the pre-existing 009 pattern): without a Docker
// daemon the case skips — it never passes vacuously and never fails the unit
// layer. Unit tests therefore cannot require a middleware daemon.
func TestT085UnitLayerNeedsNoMiddleware(t *testing.T) {
	root := auditRepoRoot(t)
	for _, path := range auditGoFiles(t, root) {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}
		tags := auditBuildTags(t, path)
		layer := false
		for _, tag := range auditLayerTags {
			if auditHasTag(tags, tag) {
				layer = true
				break
			}
		}
		if layer {
			continue
		}
		imports := auditImports(t, path)
		if auditImportHasPrefix(imports, "github.com/xtianxx/txharbor/internal/testutil") {
			t.Errorf("%s: unit-eligible test imports the Docker-backed testutil helpers", path)
		}
		if !auditImportHasPrefix(imports, "github.com/testcontainers/testcontainers-go") {
			continue
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		if !strings.Contains(string(body), "SkipIfProviderIsNotHealthy") &&
			!strings.Contains(string(body), "SkipIfProviderIsNotRunning") {
			t.Errorf("%s: unit-eligible test imports testcontainers without a skip guard (Docker would become a unit-layer dependency)", path)
		}
	}
}

// TestT085LayerEntryPoints asserts every layer has tagged tests and a Makefile
// entry point whose command matches the layer's build tag; the 013-added
// targets carry the require_tagged_tests guard (an empty layer must report NOT
// RUN, never a vacuous pass).
func TestT085LayerEntryPoints(t *testing.T) {
	root := auditRepoRoot(t)

	layerFiles := map[string]int{}
	for _, path := range auditGoFiles(t, root) {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}
		tags := auditBuildTags(t, path)
		for _, tag := range auditLayerTags {
			if auditHasTag(tags, tag) {
				layerFiles[tag]++
			}
		}
	}

	makefileBytes, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		t.Fatalf("ReadFile Makefile: %v", err)
	}
	makefile := string(makefileBytes)

	type targetCheck struct {
		target   string
		command  string
		guardTag string // empty = no require_tagged_tests guard expected
	}
	checks := []targetCheck{
		{target: "test", command: "go test -count=1"},
		{target: "test-integration", command: "-tags integration"},
		{target: "test-integration-redis", command: "-tags integration_redis", guardTag: "integration_redis"},
		{target: "test-integration-kafka", command: "-tags integration_kafka", guardTag: "integration_kafka"},
		{target: "test-contract", command: "-tags contract", guardTag: "contract"},
		{target: "test-e2e", command: "-tags e2e", guardTag: "e2e"},
		{target: "test-fault", command: "-tags fault", guardTag: "fault"},
		{target: "test-perf", command: "-tags perf", guardTag: "perf"},
	}
	for _, check := range checks {
		recipe := auditMakefileTarget(t, makefile, check.target)
		if !strings.Contains(recipe, check.command) {
			t.Errorf("Makefile target %s: recipe does not run %q", check.target, check.command)
		}
		if check.guardTag != "" && !strings.Contains(recipe, "require_tagged_tests,"+check.guardTag) {
			t.Errorf("Makefile target %s: missing require_tagged_tests,%s guard", check.target, check.guardTag)
		}
	}

	for _, tag := range auditLayerTags {
		if layerFiles[tag] == 0 {
			t.Errorf("layer %q has no tagged test files (empty layer)", tag)
		}
	}
}

// TestT085ComponentLayerIndependence asserts the Redis and Kafka layers stay
// separable: Redis-tagged tests never import the Kafka client, Kafka-tagged
// tests never import the Redis client, and no test file mixes both component
// tags.
func TestT085ComponentLayerIndependence(t *testing.T) {
	root := auditRepoRoot(t)
	for _, path := range auditGoFiles(t, root) {
		if !strings.HasSuffix(path, "_test.go") {
			continue
		}
		tags := auditBuildTags(t, path)
		redis := auditHasTag(tags, "integration_redis")
		kafka := auditHasTag(tags, "integration_kafka")
		if redis && kafka {
			t.Errorf("%s: mixes integration_redis and integration_kafka tags", path)
		}
		imports := auditImports(t, path)
		if redis && auditImportHasPrefix(imports, "github.com/twmb/franz-go") {
			t.Errorf("%s: Redis layer imports the Kafka client (franz-go)", path)
		}
		if kafka && auditImportHasPrefix(imports, "github.com/redis/go-redis") {
			t.Errorf("%s: Kafka layer imports the Redis client (go-redis)", path)
		}
	}
}

// TestT085FaultPerfStayOutOfPRsAndProduction asserts Fault/Perf run only in
// their independent workflow, never on ordinary PRs, and never enter the
// production build.
func TestT085FaultPerfStayOutOfPRsAndProduction(t *testing.T) {
	root := auditRepoRoot(t)

	ciBytes, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("ReadFile ci.yml: %v", err)
	}
	ci := string(ciBytes)
	for _, token := range []string{"test-fault", "test-perf"} {
		if strings.Contains(ci, token) {
			t.Errorf("ci.yml references %q: Fault/Perf must not run on ordinary PRs", token)
		}
	}

	fpBytes, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "fault-perf.yml"))
	if err != nil {
		t.Fatalf("ReadFile fault-perf.yml: %v", err)
	}
	fp := string(fpBytes)
	if !strings.Contains(fp, "schedule:") || !strings.Contains(fp, "workflow_dispatch:") {
		t.Error("fault-perf.yml: expected scheduled and manual triggers")
	}
	if strings.Contains(fp, "pull_request") {
		t.Error("fault-perf.yml: must never trigger on pull_request")
	}

	for _, path := range auditGoFiles(t, root) {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		imports := auditImports(t, path)
		for _, pkg := range []string{"internal/faultdrill", "internal/perf"} {
			if auditImportHasPrefix(imports, "github.com/xtianxx/txharbor/"+pkg) {
				t.Errorf("%s: production file imports %s (test-only layer)", path, pkg)
			}
		}
	}

	// The faultdrill and perf packages carry exactly their own layer tag; only
	// doc.go (untagged) is buildable without the tag.
	for _, dir := range []string{"faultdrill", "perf"} {
		want := map[string]string{"faultdrill": "fault", "perf": "perf"}[dir]
		entries, err := os.ReadDir(filepath.Join(root, "internal", dir))
		if err != nil {
			t.Fatalf("ReadDir internal/%s: %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
				continue
			}
			path := filepath.Join(root, "internal", dir, entry.Name())
			tags := auditBuildTags(t, path)
			if entry.Name() == "doc.go" {
				if len(tags) != 0 {
					t.Errorf("internal/%s/doc.go must stay untagged so the package builds", dir)
				}
				continue
			}
			if !auditHasTag(tags, want) {
				t.Errorf("internal/%s/%s: missing %q build tag", dir, entry.Name(), want)
			}
			for _, tag := range auditLayerTags {
				if tag != want && auditHasTag(tags, tag) {
					t.Errorf("internal/%s/%s: leaks into layer %q", dir, entry.Name(), tag)
				}
			}
		}
	}
}

// TestT085NoDebeziumCDCKubernetesDependencies asserts go.mod, compose.yaml and
// the CI workflows carry zero Debezium/CDC/Kubernetes runtime or test
// dependencies (ADR-013 §5; FR-28).
func TestT085NoDebeziumCDCKubernetesDependencies(t *testing.T) {
	root := auditRepoRoot(t)

	forbidden := []string{"debezium", "k8s.io", "kubernetes", "sigs.k8s.io", "helm.sh", "fluxcd", "strimzi", "operator-sdk", "kubebuilder", "/cdc"}

	// go.mod: check required module paths only (comments may legitimately
	// explain that these dependencies are out of scope).
	gomodBytes, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatalf("ReadFile go.mod: %v", err)
	}
	moduleLine := regexp.MustCompile(`^\s*(?:require\s+)?([a-zA-Z0-9][\w.\-]*\.[a-z]{2,}/[^\s]+)\s+v[0-9]`)
	for _, line := range strings.Split(string(gomodBytes), "\n") {
		match := moduleLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		lower := strings.ToLower(match[1])
		for _, token := range forbidden {
			if strings.Contains(lower, token) {
				t.Errorf("go.mod requires forbidden module %q (token %q)", match[1], token)
			}
		}
	}

	// compose.yaml: check every image reference.
	composeBytes, err := os.ReadFile(filepath.Join(root, "compose.yaml"))
	if err != nil {
		t.Fatalf("ReadFile compose.yaml: %v", err)
	}
	imageLine := regexp.MustCompile(`(?m)^\s*image:\s*(\S+)`)
	for _, match := range imageLine.FindAllStringSubmatch(string(composeBytes), -1) {
		lower := strings.ToLower(match[1])
		for _, token := range forbidden {
			if strings.Contains(lower, token) {
				t.Errorf("compose.yaml uses forbidden image %q (token %q)", match[1], token)
			}
		}
	}

	// CI workflows: no k8s/debezium tooling or actions.
	workflowToken := regexp.MustCompile(`(?i)\b(debezium|kubernetes|kubectl|kustomize|strimzi|helm|fluxcd|k8s|cdc)\b`)
	for _, name := range []string{"ci.yml", "fault-perf.yml"} {
		body, err := os.ReadFile(filepath.Join(root, ".github", "workflows", name))
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}
		if match := workflowToken.FindString(string(body)); match != "" {
			t.Errorf(".github/workflows/%s references forbidden tooling %q", name, match)
		}
	}

	// Go sources: no forbidden container images anywhere (test dependencies).
	// The audit scanner itself is excluded: it carries the vocabulary as
	// literals by construction.
	imageTokens := []string{"debezium/", "strimzi/", "k8s.gcr.io", "registry.k8s.io", "kindest/", "kubectl", "kustomize"}
	self := filepath.Join(root, "internal", "app", "layering_audit_test.go")
	for _, path := range auditGoFiles(t, root) {
		if path == self {
			continue
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", path, err)
		}
		lower := strings.ToLower(string(body))
		for _, token := range imageTokens {
			if strings.Contains(lower, token) {
				t.Errorf("%s references forbidden image/tooling token %q", path, token)
			}
		}
	}
}

// TestT085EventsImportBoundary asserts internal/events keeps its import
// boundary: no RPC/signer/cache/rate-limit/upstream writer imports, and the
// franz-go broker client only in the publisher and consumer runtime files.
func TestT085EventsImportBoundary(t *testing.T) {
	root := auditRepoRoot(t)
	eventsDir := filepath.Join(root, "internal", "events")
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		t.Fatalf("ReadDir internal/events: %v", err)
	}

	forbidden := []string{
		"net",
		"google.golang.org/grpc",
		"github.com/ethereum/go-ethereum",
		"github.com/xtianxx/txharbor/internal/eth",
		"github.com/xtianxx/txharbor/internal/signer",
		"github.com/xtianxx/txharbor/internal/indexer",
		"github.com/xtianxx/txharbor/internal/withdrawal",
		"github.com/xtianxx/txharbor/internal/execution",
		"github.com/xtianxx/txharbor/internal/txlifecycle",
		"github.com/xtianxx/txharbor/internal/nonce",
		"github.com/xtianxx/txharbor/internal/app",
		"github.com/xtianxx/txharbor/internal/cache",
		"github.com/xtianxx/txharbor/internal/ratelimit",
		"github.com/redis/go-redis",
	}
	brokerRuntime := map[string]bool{"publisher.go": true, "consumer.go": true}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(eventsDir, entry.Name())
		imports := auditImports(t, path)
		for _, prefix := range forbidden {
			if auditImportHasPrefix(imports, prefix) {
				t.Errorf("internal/events/%s imports forbidden package %q", entry.Name(), prefix)
			}
		}
		if auditImportHasPrefix(imports, "github.com/twmb/franz-go") && !brokerRuntime[entry.Name()] {
			t.Errorf("internal/events/%s imports franz-go outside the publisher/consumer runtime", entry.Name())
		}
	}
}
