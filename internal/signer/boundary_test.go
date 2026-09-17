package signer

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// boundary_test.go owns spec task T023 (US5; FR-01/FR-20, plan Structure
// Decision): the signer is a signer, not an indexer and not a broadcaster.
// Its production import graph must never reach 007/008 writers or any RPC
// dial/broadcast package, and only the dedicated signer-serve process may
// construct a KeyProvider — the business `serve` path must not, or deployment
// would silently bind to a local test key.

// forbiddenDeps are import paths the signer package must never reach, even
// transitively: 007/008 writers own withdrawal/indexer state and pull in the
// chain side of the system, and the geth RPC/dial packages would make the
// signer capable of broadcasting (FR-01: signing, never sending).
var forbiddenDeps = []string{
	"github.com/xtianxx/txharbor/internal/withdrawal",
	"github.com/xtianxx/txharbor/internal/indexer",
	"github.com/ethereum/go-ethereum/rpc",
	"github.com/ethereum/go-ethereum/ethclient",
	"github.com/ethereum/go-ethereum/accounts/abi/bind",
	"net/rpc",
}

// TestBoundaryImportGraph asserts the transitive production import graph via
// `go list -deps`. Transitive (not just direct) matters: a helper import must
// not smuggle the chain side in behind a single hop.
func TestBoundaryImportGraph(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go tool not available: %v", err)
	}
	// `.` is this package's directory under `go test`.
	out, err := exec.Command(goBin, "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps .: %v", err)
	}
	deps := strings.Fields(string(out))
	if len(deps) == 0 {
		t.Fatalf("go list -deps returned no dependencies; boundary unverified")
	}
	for _, dep := range deps {
		for _, bad := range forbiddenDeps {
			if dep == bad {
				t.Errorf("internal/signer transitively imports forbidden package %q", bad)
			}
		}
	}
}

// TestBoundaryProviderConstruction pins the only two non-test source files
// allowed to name the dev key provider: provider.go defines it, signerserve.go
// is the one process that builds it. Any other construction site — business
// `serve` in particular — fails here.
func TestBoundaryProviderConstruction(t *testing.T) {
	repoRoot := filepath.Join("..", "..")
	re := regexp.MustCompile(`NewDevKeyProvider\s*\(`)
	found := map[string]int{}
	for _, dir := range []string{filepath.Join(repoRoot, "internal"), filepath.Join(repoRoot, "cmd")} {
		err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if n := len(re.FindAll(body, -1)); n > 0 {
				rel, relErr := filepath.Rel(repoRoot, path)
				if relErr != nil {
					return relErr
				}
				found[filepath.ToSlash(rel)] += n
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	allowed := map[string]bool{
		"internal/app/signerserve.go": true,
		"internal/signer/provider.go": true,
	}
	for file, n := range found {
		if !allowed[file] {
			t.Errorf("KeyProvider constructed in %s (%d refs): only signer-serve may build one", file, n)
		}
	}
	for file := range allowed {
		if found[file] == 0 {
			t.Errorf("expected a KeyProvider reference in %s; boundary would silently pass if it moved", file)
		}
	}
}
