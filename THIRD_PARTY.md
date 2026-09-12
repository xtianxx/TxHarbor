# Third-Party Notices

Scope: 001-project-foundation. Verified against the module versions pinned in
`go.mod` on 2026-09-12. This file satisfies task T040.

## Direct dependencies

| Module | Version | License | Use |
|---|---|---|---|
| github.com/ethereum/go-ethereum | v1.17.5 | LGPL-3.0 (library, see below) | JSON-RPC client + chain-id check |
| github.com/jackc/pgx/v5 | v5.11.0 | MIT | PostgreSQL pool + `database/sql` bridge |
| github.com/moby/moby/api | v1.55.0 | Apache-2.0 | test-only (integration build tag): container/network types for fixed host-port bindings |
| github.com/pressly/goose/v3 | v3.28.0 | MIT | versioned migrations |
| github.com/prometheus/client_golang | v1.24.1 | Apache-2.0 | `/metrics` exposition |
| github.com/testcontainers/testcontainers-go (+ modules/postgres) | v0.44.0 | MIT | test-only (integration tests) |

License texts ship with each module in the Go module cache and are reproduced
by `go mod download`; no code is vendored in this repository.

## go-ethereum (v1.17.5) — referenced package inventory

`go list -deps ./...` and `go list -deps -tags integration ./...` report the
following go-ethereum packages in the build closure (transitive imports
included):

```
github.com/ethereum/go-ethereum
github.com/ethereum/go-ethereum/common
github.com/ethereum/go-ethereum/common/bitutil
github.com/ethereum/go-ethereum/common/hexutil
github.com/ethereum/go-ethereum/common/math
github.com/ethereum/go-ethereum/common/mclock
github.com/ethereum/go-ethereum/core/types
github.com/ethereum/go-ethereum/core/types/bal
github.com/ethereum/go-ethereum/crypto
github.com/ethereum/go-ethereum/crypto/keccak
github.com/ethereum/go-ethereum/crypto/kzg4844
github.com/ethereum/go-ethereum/crypto/secp256k1
github.com/ethereum/go-ethereum/ethclient
github.com/ethereum/go-ethereum/internal/telemetry
github.com/ethereum/go-ethereum/log
github.com/ethereum/go-ethereum/metrics
github.com/ethereum/go-ethereum/p2p/netutil
github.com/ethereum/go-ethereum/params
github.com/ethereum/go-ethereum/params/forks
github.com/ethereum/go-ethereum/rlp
github.com/ethereum/go-ethereum/rlp/internal/rlpstruct
github.com/ethereum/go-ethereum/rpc
```

### License conclusion

- Upstream ships two license files: `COPYING` (GPL-3.0) applies to code under
  `cmd/`; `COPYING.LESSER` (LGPL-3.0) applies to the library, i.e. all code
  outside `cmd/`.
- Every referenced package above is outside `cmd/` (verified: the dependency
  closure contains zero `github.com/ethereum/go-ethereum/cmd/...` packages in
  both the unit and integration build tags).
- Package source headers state the license explicitly, e.g.
  `rpc/client.go` and `ethclient/ethclient.go` carry "GNU Lesser General
  Public License".
- **No GPL-3.0 (cmd/) code is linked. No distribution blocker.**

### Binary distribution strategy

TxHarbor statically links the unmodified go-ethereum library packages under
LGPL-3.0. For any distributed binary:

1. Include this notice plus the upstream `COPYING.LESSER` text (and licenses
   of the other direct dependencies above).
2. State the exact upstream version (v1.17.5) and its public source location.
3. Do not modify go-ethereum sources; if a patch is ever required, publish it
   under LGPL-3.0 and provide a way to relink (rebuild from source with the
   patched module) as required by LGPL-3.0 §4/§6.

If a future feature pulls in a package under go-ethereum's `cmd/` tree
(GPL-3.0), distribution must be blocked and escalated before shipping.
