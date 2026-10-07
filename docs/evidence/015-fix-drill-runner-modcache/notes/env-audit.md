# Drill runner container environment audit — failed run 37298994567

Repo: /home/dream/product_env/TxHarbor (Go monorepo, worktree at HEAD=2c1ccd3; failing revision be272cb7).
Scope: `scripts/drillcoverage/run-in-container.sh`, `.github/workflows/drill.yml`, `Makefile` test-drill; plus
local controlled experiments E1-E4 run with /tmp scratch only (real module cache and repo untouched).
Auditor: EnvAudit (delegated worker). All experiments on 2026-10-06, host go1.26.5, docker 29.6.1.

**Worktree note (not authored by this task):** `git status` shows ` M scripts/drillcoverage/run-in-container.sh`,
worktree sha256 `af9955e49d5f5e3c655b045e34e50d45a890505de116d05810b0c20b0ca4f68e`, mtime 2026-10-06 19:57:40 +0800.
It adds 15 lines after HEAD line 36 (a host-side `go mod download` priming block, worktree lines 37-51).
The committed HEAD/be272cb blob is the 144-line revision, sha256
`ccadbec71a4f156344812ebc1822957579c3d50476f6b0b275dc9597c32b97f2`, and contains NO priming step.
**All line cites below use the 144-line HEAD/be272cb numbering** (the revision that failed in production); the
worktree adds +15 to every line ≥37 (e.g. mounts move from 54-61 to 69-76) and its delta is quoted in §6.
No repo file was edited by this audit; the ` M` entry above predates/parallels this task and was left untouched.

---

## 0. Host facts (for replicating the experiments)

```
$ go env GOMODCACHE GOROOT GOVERSION
/home/dream/go/pkg/mod
/usr/local/go
go1.26.5
$ id -u; id -g            -> 1000 / 1000
$ stat -c '%g %a %U:%G' /var/run/docker.sock -> 989 660 root:docker
$ docker --version        -> Docker version 29.6.1, build 8900f1d
$ docker image inspect postgres@sha256:4ef4dbc9...2280 --format '{{.Id}} {{.Size}}'
sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280 162457547
$ go env GOPROXY GOFLAGS GOSUMDB -> https://goproxy.cn,direct / (empty) / sum.golang.org
```

Real cache already contains BOTH modules (pre-existing, not fetched by me):
- extracted: `/home/dream/go/pkg/mod/github.com/decred/dcrd/dcrec/secp256k1/v4@v4.0.1` (mtime Sep 1),
  `/home/dream/go/pkg/mod/github.com/testcontainers/testcontainers-go/modules/postgres@v0.44.0` (mtime Sep 12)
- download: `.../cache/download/github.com/decred/dcrd/dcrec/secp256k1/v4/@v/{v4.0.1.info,.mod,.zip(747302B),.ziphash,list,.lock}`
  and `.../cache/download/github.com/testcontainers/testcontainers-go/modules/postgres/@v/{v0.44.0.*, .zip(23685B)}`.
Real cache size `du -sh` = 3.4G. Post-experiment re-check: all four paths still present (nothing mutated).

Experiment harness: `/tmp/txharbor-experiments/in-container.sh` reproduces the wrapper's docker invocation
byte-for-byte in mount/identity/env terms (see §1) with only these deltas: `TXHARBOR_DRILL_METADATA` omitted,
`TXHARBOR_TESTED_TREE_FINGERPRINT` passed empty, image/args otherwise identical. Raw outputs:
`/tmp/txharbor-experiments/{e1-smoke,e1a,e1a-prime,e1b,e1c,e2-download,e2a,e2a-prime,e2a-cold,e2b,e4}.out`,
`/tmp/txharbor-experiments/e3-download.json`; caches `/tmp/txharbor-modcache-pruned`, `/tmp/txharbor-modcache-primed`.

---

## 1. Part 1 — Assembly audit (static; lines = 144-line revision)

### 1.1 Identity + mounts + env

| # | Host source | Container dest | Line | Mode | Notes |
|---|-------------|----------------|------|------|-------|
| 1 | `$REPO_ROOT` (checkout) | `/workspace` | 54 | **ro** | source tree; `docker run` cwd not set, script `cd`s inside entrypoint |
| 2 | `$(go env GOROOT)` = /usr/local/go | `/host-go` (`GOROOT_CONTAINER`, line 17) | 55 | **ro** | pinned toolchain binaries/pkgs (go1.26.5) |
| 3 | `$(go env GOMODCACHE)` = /home/dream/go/pkg/mod (CI: /home/runner/go/pkg/mod, log:189) | `/host-gomodcache` (`GOMODCACHE_CONTAINER`, line 18) | 56 | **ro** | module sources + `cache/download` |
| 4 | `readlink -f $(command -v docker)` | same abs path | 57 | **ro** | file mount |
| 5 | `readlink -f $(command -v make)` | same abs path | 58 | **ro** | file mount |
| 6 | `$DOCKER_SOCKET` (default /var/run/docker.sock) | `/var/run/docker.sock` | 59 | **rw** (no `readonly`) | testcontainers talks to host daemon |
| 7 | `$SCRATCH_HOST` = `mktemp -d ${TMPDIR:-/tmp}/txharbor-drill.XXXXXX` (line 43; subdirs `gocache`/`tmp`/`home`, line 45) | `/drill-scratch` | 60 | **rw** | GOCACHE/TMPDIR/HOME root |
| 8 | `$EVIDENCE_HOST` = `$TXHARBOR_DRILL_EVIDENCE_DIR` (CI: `${{ runner.temp }}/drill-evidence`, drill.yml:83) | `/drill-evidence` | 61 | **rw** | run records + metadata |

Execution identity (lines 51-53): `--user "$RUNNER_UID:$RUNNER_GID"` (`id -u`/`id -g`, lines 22-23) →
in-container uid:gid **1000:1000** (CI: runner user); `--group-add "$DOCKER_SOCKET_GID"` (line 24; 989 here)
→ supplementary group so the socket is usable; `--add-host=host.docker.internal:host-gateway`.
Smoke proof (raw): `uid=1000 gid=1000 groups=1000,989`; `drwxr-xr-x 1000 1000 /drill-evidence`;
`drwxr-xr-x 1000 1000 /host-gomodcache`; `drwxr-xr-x root root /host-go`; `/workspace` 1000:1000.

Env (`-e`, lines 62-80): `PATH` (62), `GOROOT=/host-go` (63), `GOMODCACHE=/host-gomodcache` (64),
`CGO_ENABLED=0` (67, with comment 65-66: minimal image has no C compiler), `GOCACHE=/drill-scratch/gocache` (68),
`TMPDIR=/drill-scratch/tmp` (69), `HOME=/drill-scratch/home` (70), `DOCKER_HOST=unix:///var/run/docker.sock` (71),
`TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal` (72), `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock` (73),
`CI=true` (74), `TXHARBOR_REQUIRE_DOCKER=1` (75), `TXHARBOR_DRILL_EVIDENCE_DIR=/drill-evidence` (76),
`TXHARBOR_TESTED_TREE_FINGERPRINT` (77), `TXHARBOR_DRILL_METADATA=/drill-evidence/<wrapper.XXXXXX>/metadata.json` (78),
`TXHARBOR_DRILL_CONTAINER_SMOKE=$SMOKE_ONLY` (79), `IMAGE_DIGEST` (80); passthrough loop for
`TXHARBOR_PG_DSN TXHARBOR_RPC_URL TXHARBOR_CHAIN_ID TXHARBOR_COMMIT` (lines 83-85).
No `GOPATH`/`GOPROXY`/`GOFLAGS`/`GOTOOLCHAIN` is set → container-go defaults (GOPATH=$HOME/go, GOPROXY=proxy.golang.org,direct).
The wrapper's own metadata file self-reports the same modes (line 125): workspace/go_root/module_cache = read-only,
scratch/evidence/docker_socket = read-write. The artifact confirms it:
`artifact-.../wrapper.IzHGk2/metadata.json` `mount_modes` block.

### 1.2 Write classification per directory (who writes what)

Container process identity = 1000:1000 (+ socket gid); ro bind mounts are unwritable regardless of uid (minimal image has no matching passwd entry; uid 1000 happens to own the host dirs but the bind is ro).

| Dir | Toolchain MUST write? | Writer / purpose | Verdict |
|-----|----------------------|------------------|---------|
| `/workspace` | No (for the drill command set) | read-only source of truth; `make test-drill` writes only to `$TXHARBOR_DRILL_EVIDENCE_DIR` and TMPDIR; `go run scripts/drillcoverage/check.go` (script lines 129/136 host-side, container runs `go test ./scripts/drillcoverage` line 121, `make -n` line 120) writes build/test temp only | **ro is compatible.** Caveat: a bare `go build ./cmd/txharbor` (no `-o`) writes the executable into cwd; on ro workspace that fails *after* successful compilation (E2-first-attempt raw below). The wrapper never does this. |
| `/host-go` | No | compiler/asm/link binaries and stdlib are read; Go never writes inside GOROOT | **ro fine.** `go.mod` go directive = 1.26.5 == GOROOT version → no `golang.org/toolchain` switch needed. |
| `/host-gomodcache` | **Only on cache miss** (module download + zip extraction + per-`@v` lock/`.info`/`.ziphash` creation). On a complete cache: zero writes (proved E2/E4). | `mkdir cache/download/<path>` is attempted by the loader for any module source or zip not already extracted | **ro requires the cache to be pre-materialized for the full build graph** — this is the production failure. |
| `/drill-scratch` | **Yes, mandatory** | `GOCACHE=/drill-scratch/gocache` (line 68) receives every compiled package/archive; `TMPDIR=/drill-scratch/tmp` (69) receives `go-build*/bNNN` work dirs, link temp, and `go test`'s compiled test binaries; `HOME` (70) backs default GOPATH and go env config | rw by design — correct. |
| `/drill-evidence` | Yes (drill layer) | Makefile creates `$TXHARBOR_DRILL_EVIDENCE_DIR/run.XXXXXX` (Makefile 77-79), writes `go-test.json` + `coverage.json` (Makefile 87-89); drill tests write S12 records there; wrapper writes `metadata.json` (line 126) and `wrapper.XXXXXX/tree-validation.json` (line 138-139) | rw by design — correct. |
| docker socket | n/a (IPC, not fs content) | testcontainers create/inspect containers on host daemon | rw by design; gid 989 via `--group-add`. |

Toolchain write summary: the go build/test toolchain MUST have writable **GOCACHE** and **TMPDIR**
(both under `/drill-scratch`) for any compile/link/test; it needs **GOMODCACHE writable only when something
is missing** — extraction and download entries are the only writes, and a complete cache eliminates them (E2/E4).
GOROOT and workspace are never written by the drill command set; evidence is written by the drill/checker layer.

Makefile test-drill (lines 74-90): `require_tagged_tests` guard (75), evidence run dir (77-84),
`go test -json -tags drill -count=1 -timeout 120m ./... > "$events"` (87) — note the JSON stream goes to the
rw evidence dir, not cwd; `go run scripts/drillcoverage/check.go` (89) — its own binary builds under TMPDIR.
Workflow drill.yml: schedule/manual only (44-48), `timeout-minutes: 240` job (67), go-version 1.26.5 (72),
step "full recovery drill layer (pinned native-tool runner)" (81-86) invoking the wrapper,
evidence upload artifact (88-95). The failing run: event=schedule, step exit inside the container.

---

## 2. Production failure signature (from `.evidence/drill-runner-env/logs/run-37298994567-full.log`)

- setup-go cache HIT: log:150 `Cache hit for: setup-go-Linux-x64-ubuntu24-go-1.26.5-c27b1067...`; log:154
  `Cache Size: ~348 MB (365111547 B)`; log:157 restored; log:189 `GOMODCACHE='/home/runner/go/pkg/mod'`.
  The restored cache contained **neither** module (that is why the loader tried to download them).
- log:370 (container) `go: downloading github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1`
- log:372 / 375 (+ repeats): `.../go-ethereum@v1.17.5/crypto/signature_nocgo.go:28:2: mkdir /host-gomodcache/cache/download/github.com/decred: read-only file system`
  (same at :29:2) — the CGO_ENABLED=0 branch of go-ethereum imports decred.
- log:471: `internal/recovery/borrowed-transport-phase1_linux_test.go:31:2: mkdir /host-gomodcache/cache/download/github.com/testcontainers/testcontainers-go/modules/postgres: read-only file system`
- log:474: `FAIL	github.com/xtianxx/txharbor/internal/recovery [setup failed]`; 16 distinct packages FAIL [setup failed].
- Tally: **31** `read-only file system` lines = 30 × decred + 1 × postgres; the only two distinct paths are
  `cache/download/github.com/decred` and `cache/download/github.com/testcontainers/testcontainers-go/modules`.

---

## 3. Part 2 — Experiments (exact commands, raw output, exit codes, timings)

Common invocation (harness, mirrors wrapper mounts/identity/env; `$MC` = cache dir mounted ro at
`/host-gomodcache`, `$SC` = rw scratch at `/drill-scratch`):
`/tmp/txharbor-experiments/in-container.sh "$MC" "$SC" '<cmd>'` — an exact-arg `docker run` of
`postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280` with
`--rm --init --user 1000:1000 --group-add 989 --add-host=host.docker.internal:host-gateway`,
ro bind mounts for /workspace,/host-go,/host-gomodcache,docker-cli,make; rw for docker.sock,/drill-scratch,/drill-evidence,
and the `-e` set of lines 62-80.

### E1 — controlled negative: real cache copy with ONLY the two modules removed

Prep: `cp -al /home/dream/go/pkg/mod /tmp/txharbor-modcache-pruned` (20.9 s; hardlinks, 3.4G apparent; the copy's
directories were chmod u+w in the COPY only before deletion — host cache re-verified intact afterwards).
Then removed from the copy: `github.com/decred` (both extracted dir and cache/download tree),
`github.com/testcontainers/testcontainers-go/modules/postgres@v0.44.0` and its `cache/download/.../modules/postgres`.
Post-state: `absent: ...` for all four (checked); neighbors kafka/redis/redpanda kept.

E1a (exit probe of the build path):
```
$ in-container.sh pruned <scratch> 'cd /workspace && CGO_ENABLED=0 go build ./cmd/txharbor'
go: downloading github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1
/host-gomodcache/github.com/ethereum/go-ethereum@v1.17.5/crypto/signature_nocgo.go:28:2: mkdir /host-gomodcache/cache/download/github.com/decred: read-only file system
/host-gomodcache/github.com/ethereum/go-ethereum@v1.17.5/crypto/signature_nocgo.go:29:2: mkdir /host-gomodcache/cache/download/github.com/decred: read-only file system
exit=1   real 3.3 s
```
Identical command with `-o /dev/null` (isolates the module failure from cwd writing), E1a′:
```
same two EROFS lines; exit=1   real 2.2 s
```

E1b (exit probe of the drill-tag test compile):
```
$ in-container.sh pruned <scratch> 'cd /workspace && CGO_ENABLED=0 go test -c -tags drill -o /dev/null ./internal/recovery'
go: downloading github.com/testcontainers/testcontainers-go/modules/postgres v0.44.0
go: downloading github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1
FAIL	github.com/xtianxx/txharbor/internal/recovery [setup failed]
FAIL
# github.com/xtianxx/txharbor/internal/recovery
internal/recovery/borrowed-transport-phase1_linux_test.go:31:2: mkdir /host-gomodcache/cache/download/github.com/testcontainers/testcontainers-go/modules/postgres: read-only file system
exit=1   real 2.6 s
```
→ Both production error signatures reproduced verbatim, with the identical docker argv/env as the wrapper.
The only cache difference vs. the positive runs is the presence of those two modules.

E1c — **controlled positive**, same pruned cache with ONLY the two modules restored (cp -a of exactly the four
paths from the real cache; nothing else changed):
```
$ in-container.sh pruned-restored <cold scratch> 'cd /workspace && CGO_ENABLED=0 go build -o /dev/null ./cmd/txharbor; echo BUILD_EXIT=$?; CGO_ENABLED=0 go test -c -tags drill -o /dev/null ./internal/recovery; echo TESTC_EXIT=$?'
BUILD_EXIT=0
TESTC_EXIT=0
docker exit=0   real 58.0 s (cold GOCACHE, both commands)
```
→ Causality isolated: exactly those two missing modules explain the failure; restoring them fixes it.

### E2 — mechanism fix: host-side `go mod download` into a fresh cache, mounted read-only

Prep:
```
$ rm -rf /tmp/txharbor-modcache-primed; mkdir -p /tmp/txharbor-modcache-primed   # 0 bytes before
$ env GOMODCACHE=/tmp/txharbor-modcache-primed go mod download                    # run in repo, host, network on
exit=0   real 9.969 s   (host GOPROXY=https://goproxy.cn,direct — mirror-dependent)
stdout: empty (matches help: "By default, download writes nothing to standard output.")
after: du -sh = 716M; 91 extracted module dirs; 91 cache/download *.zip files
```
Both target modules materialized BOTH ways:
```
/tmp/txharbor-modcache-primed/github.com/decred/dcrd/dcrec/secp256k1/v4@v4.0.1
/tmp/txharbor-modcache-primed/github.com/testcontainers/testcontainers-go/modules/postgres@v0.44.0
/tmp/.../cache/download/github.com/decred/dcrd/dcrec/secp256k1/v4/@v/{v4.0.1.info,.mod,.zip(747302B),.ziphash,list,.lock}
/tmp/.../cache/download/github.com/testcontainers/testcontainers-go/modules/postgres/@v/{v0.44.0.info,.mod,.zip(23685B),.ziphash,list,.lock}
$ GOMODCACHE=/tmp/txharbor-modcache-primed go mod download -json   (exit=0, stderr 0 bytes)
entries: 91 | with Error: 0 | with Dir: 91 | with Zip: 91 | with GoMod: 91
github.com/testcontainers/testcontainers-go/modules/postgres v0.44.0 | Dir: .../postgres@v0.44.0 | Zip: .../v0.44.0.zip
github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 | Dir: .../secp256k1/v4@v4.0.1 | Zip: .../v4.0.1.zip
```
So no-arg `go mod download` DOES extract sources (`Dir`) and populate download entries (`Zip`) — the fix design
does not need a separate extraction step. (The "go mod download does not materialize" abort condition did not trigger.)

Docker positives against the primed cache mounted read-only — same argv as E1:
```
E2a′: 'cd /workspace && CGO_ENABLED=0 go build -o /dev/null ./cmd/txharbor'                    exit=0  real 3.9 s (warm)
E2a″: same, fresh GOCACHE                                                                      exit=0  real 43.7 s (cold)
E2b : 'cd /workspace && CGO_ENABLED=0 go test -c -tags drill -o /dev/null ./internal/recovery' exit=0  real 19.2 s (exit 0, no output)
```
E2-first-attempt nuance (raw, recorded for accuracy): the very first E2a ran `go build ./cmd/txharbor` **without `-o`**
and failed with:
```
github.com/xtianxx/txharbor/cmd/txharbor: go build github.com/xtianxx/txharbor/cmd/txharbor: copying /drill-scratch/tmp/go-build315287469/b001/exe/a.out: open txharbor: read-only file system
exit=1   real 35.1 s
```
i.e. compilation of the entire graph against the read-only primed cache succeeded; only the final copy of the
executable into the read-only cwd `/workspace` failed. That is an artifact of the probe command, not of the drill
workflow (Makefile uses `go test`, never bare `go build` in the workspace). With `-o /dev/null` it is exit 0.

### E4 — full drill-tag import graph + vet, primed cache ro

```
$ in-container.sh primed <cold scratch> 'set +e
cd /workspace
CGO_ENABLED=0 go list -deps -tags drill ./... >/dev/null 2>/drill-scratch/list.err
echo "LIST_EXIT=$?"; echo "list.err bytes: $(wc -c < /drill-scratch/list.err)"
echo "read-only errors: $(grep -c "read-only file system" /drill-scratch/list.err)"
CGO_ENABLED=0 go vet -tags drill ./internal/recovery 2>&1; echo "VET_EXIT=$?"'
LIST_EXIT=0
list.err bytes: 0
read-only errors: 0
VET_EXIT=0
docker exit=0   real 51.9 s (cold; vet was cheap)
```
→ The whole `./...` package graph under `-tags drill` resolves with zero stderr and zero EROFS attempts, and
`go vet -tags drill ./internal/recovery` (which type-checks the drill-tagged test files incl. the testcontainers
import) passes.

---

## 4. E3 — `go help mod download` semantics (go1.26.5, verbatim excerpt)

```
usage: go mod download [-x] [-json] [-reuse=old.json] [modules]

Download downloads the named modules, which can be module patterns selecting
dependencies of the main module or module queries of the form path@version.

With no arguments, download applies to the modules needed to build and test
the packages in the main module: the modules explicitly required by the main
module if it is at 'go 1.17' or higher, or all transitively-required modules
if at 'go 1.16' or lower.

The go command will automatically download modules as needed during ordinary
execution. The "go mod download" command is useful mainly for pre-filling
the local cache or to compute the answers for a Go module proxy.

By default, download writes nothing to standard output. It may print progress
messages and errors to standard error.
```

Module set, precisely: `go.mod` line 3 `go 1.26.5` ⇒ ≥1.17 branch ⇒ **the modules explicitly required by the
main module** = every entry of every `require` block (direct AND the ones annotated `// indirect`), plus their
go.mod files. Empirically: `go mod edit -json` reports **91** require entries, and the no-arg download produced
**91** extracted dirs + **91** zips (E2). This set is exactly what the read-only-mounted cache must contain for
any package/tag/GOOS configuration reachable from the main module (graph pruning means the tidied require list is
the complete module-level superset for this main module).

Both failing modules are explicit requires:
- `go.mod:29` `github.com/testcontainers/testcontainers-go/modules/postgres v0.44.0` (direct require block, 22-37)
- `go.mod:55` `github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect` (indirect require block)
- go.sum: `go.mod:80-81` (secp256k1 h1 + /go.mod) and `go.mod:298-299` (modules/postgres h1 + /go.mod)
`vendor/` does not exist, and GOFLAGS is empty in the wrapper, so the module cache is authoritative.

---

## 5. Part 2 — E4 result (see §3-E4): exit 0, 0-byte stderr, 0 EROFS attempts, vet exit 0.

---

## 6. Assessment

**Necessary and sufficient single host-side step:** run `go mod download` (no arguments) on the host, with the
default `GOMODCACHE` — i.e. the exact directory that will be bind-mounted read-only — *before* the
`docker run`, so the cache is materialized (extracted sources + `cache/download` entries) for the whole
CGO_ENABLED=0 + `drill`-tag build graph. Controlled evidence: removing only those two modules from an otherwise
complete cache reproduces both production `mkdir ... read-only file system` signatures (E1a/E1b) while restoring
only those two makes the identical docker argv succeed (E1c, BUILD_EXIT=0 / TESTC_EXIT=0); priming a fresh empty
cache with no-arg `go mod download` (9.97 s, 0 → 716 MB, 91 modules, all with `Dir`+`Zip`) likewise yields
exit 0 for `go build`, `go test -c -tags drill`, full `go list -deps -tags drill ./...` and `go vet -tags drill`
(E2/E4). The uncommitted worktree candidate implements exactly this (worktree lines 37-51, inserted after HEAD
line 36, with `if ! go mod download; then echo ...; exit 1; fi`); the failing revision be272cb has no such step.

**Residual risks:** (1) completeness of the fix equals tidiness of `go.mod` — the download set is the require
set, so a needed module missing from `go.mod` would still fail (nothing drill-specific, `go mod tidy` discipline);
(2) toolchain switching is not exercised: `go.mod` go 1.26.5 == pinned GOROOT 1.26.5, but if the directive were
bumped above the workflow's `go-version`, the host priming step would fetch `golang.org/toolchain` into the same
cache and the container's `GOTOOLCHAIN=auto` would need to execute it from the read-only mount (likely works;
untested [INFERENCE]); (3) the priming step requires host network/proxy and adds the missing modules to the
setup-go cache (fresh-prime size 716 MB vs the 348 MB restored CI cache; the delta is what must be cached);
(4) `GOPROXY`/`GOFLAGS` are inherited from the host env for the priming call (host here had a mirror,
`https://goproxy.cn,direct`) — behavior is default-policy dependent, not repo-pinned; (5) modules needed only by
other configurations (e.g. future cgo-enabled or platform-specific variants) are covered only insofar as they are
listed in `go.mod` requires.

Non-blocking observation: in the failing run the wrapper's read-only module-cache mount plus a CI cache that
predates the two modules is the entire mechanism; no evidence of concurrent cache mutation or permission
mismatch was found (uid/gid and socket group wiring verified in-container).

---

## 7. Repro index (scratch, not repo)

- Harness: `/tmp/txharbor-experiments/in-container.sh` (mirror of wrapper docker args).
- Caches: `/tmp/txharbor-modcache-pruned` (E1/E1c), `/tmp/txharbor-modcache-primed` (E2/E3/E4).
- Raw logs: `e1-smoke.out`, `e1a.out`, `e1a-prime.out`, `e1b.out`, `e1c.out`, `e2-download.out`, `e2a.out`,
  `e2a-prime.out`, `e2a-cold.out`, `e2b.out`, `e4.out`, `e3-download.json`, `e3-download.err` in same dir.
- All experiment commands were executed with the repo untouched (git status shows only the pre-existing
  ` M scripts/drillcoverage/run-in-container.sh` documented at the top) and the real GOMODCACHE untouched.
