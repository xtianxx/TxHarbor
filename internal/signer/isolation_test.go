package signer

import (
	"net"
	"strconv"
	"strings"
	"testing"
)

// 009 isolated validation resources (T003; quickstart.md "Isolation scheme").
// Disjoint from 008 (txharbor_008 / 55432 / 58545) and from the repo defaults
// (5432 / 8545). Automated tests stay on per-test testcontainers with random
// published ports; these values apply to manual compose/DSN walkthroughs only.
const (
	isolationDBName     = "txharbor_009"
	isolationPGHostPort = 5433
	isolationHTTPAddr   = "127.0.0.1:8091"
	isolationPGVolume   = "txharbor_009_pgdata"
)

// TestIsolationResourcesDisjoint fails if a 009 validation resource collides
// with a 008 resource or a repo default -- parallel 008/009 runs must never
// share a database, port or volume (T003 done condition).
func TestIsolationResourcesDisjoint(t *testing.T) {
	for name, why := range map[string]string{
		"txharbor":     "the shared default database",
		"txharbor_008": "the 008 worktree database",
	} {
		if isolationDBName == name {
			t.Errorf("009 database %q collides with %s", isolationDBName, why)
		}
	}
	if !strings.HasSuffix(isolationDBName, "_009") {
		t.Errorf("009 database %q is not workdir-local to 009", isolationDBName)
	}
	if isolationPGVolume == "pgdata" || strings.HasSuffix(isolationPGVolume, "_008_pgdata") {
		t.Errorf("009 PG volume %q collides with a shared/008 volume", isolationPGVolume)
	}

	forbiddenPorts := map[int]string{
		0:     "an unset port",
		5432:  "the default PostgreSQL port",
		55432: "the 008 PostgreSQL port",
		58545: "an 008 listener port",
		8545:  "the default Anvil/RPC port",
		8080:  "the default HTTP port",
	}
	if why, bad := forbiddenPorts[isolationPGHostPort]; bad {
		t.Errorf("009 PG port %d collides with %s", isolationPGHostPort, why)
	}

	host, portStr, err := net.SplitHostPort(isolationHTTPAddr)
	if err != nil {
		t.Fatalf("isolationHTTPAddr %q: %v", isolationHTTPAddr, err)
	}
	if host != "127.0.0.1" {
		t.Errorf("009 HTTP host %q must be loopback only", host)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("isolationHTTPAddr port %q: %v", portStr, err)
	}
	if why, bad := forbiddenPorts[port]; bad {
		t.Errorf("009 HTTP port %d collides with %s", port, why)
	}
	if port == isolationPGHostPort {
		t.Errorf("009 HTTP port %d collides with the 009 PG port", port)
	}
}
