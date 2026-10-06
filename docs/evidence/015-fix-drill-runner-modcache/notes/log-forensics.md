# Run 37298994567 log forensics

Line numbers for the GitHub Actions log below are global lines in `.evidence/drill-runner-env/logs/run-37298994567-full.log`. `go-test.json` and `coverage.json` line numbers refer to the downloaded artifacts. No drill was rerun.

## Timeline

- The workflow step is `full recovery drill layer (pinned native-tool runner)` and runs `scripts/drillcoverage/run-in-container.sh` (`.github/workflows/drill.yml:81-85`). The log starts that script at lines 289-290 (10:49:33.543Z).
- Docker begins at log lines 300-301 (10:49:34.050Z / 10:49:34.546Z): the pinned PostgreSQL image is absent locally and is pulled. The image pull completes before tool validation; `pg_restore` and `pg_dump` 18.6, `go test ./scripts/drillcoverage`, and Docker connectivity all pass at lines 365-368 (10:49:42.055Z–10:49:49.634Z).
- The first visible module-fetch notices are lines 369-370 (10:49:49.896Z–10:49:49.910Z). The `test-drill` Make recipe runs `go test -json -tags drill -count=1 -timeout 120m ./...`, then runs `scripts/drillcoverage/check.go` over the captured events (`Makefile:75-90`). The checked-out wrapper invokes `make test-drill` at `scripts/drillcoverage/run-in-container.sh:146`.
- First build failure: log line 371 begins the `cmd/txharbor` build output; line 372 (10:50:29.208Z) reports the first `read-only file system` mkdir error. In the raw Go event artifact, `cmd/txharbor` is marked `[setup failed]` / `Action: fail` at `go-test.json:8-9` (Go event time 10:49:50.383Z).
- Last raw Go test event is the passing `scripts/drillcoverage` package event at `go-test.json:1263` (10:50:29.049Z). The checker's final required subtest diagnostic is log line 1678; it exits 1 at line 1679, Make reports `Error 1` at line 1680, and the workflow reports step exit code 2 at line 1681. Artifact upload and cleanup follow; the full log's final line 1744 is an unrelated Node.js deprecation warning.
- The wrapper calculates a pre-run fingerprint (`scripts/drillcoverage/run-in-container.sh:52-56`) and records a post-run stability result (`:148-159`). `wrapper.IzHGk2/tree-validation.json:1` records `source_tree_stable:true` and identical before/after `sha256:cce2675fea93b9062474d51e4c563d6ffcc3850c675da4967543e7b73e45dda9`; `coverage.json:4` records the same fingerprint. No standalone fingerprint line is printed in the run log.

## Failure and cache evidence

- The two module paths are listed in `go.mod`: `github.com/testcontainers/testcontainers-go/modules/postgres v0.44.0` at line 29 and `github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect` at line 55. Go logs attempted downloads at log lines 369-370.
- The Decred import causes two read-only cache errors per affected package: `signature_nocgo.go:28:2` at log lines 372, 381, 390, 399, 408, 417, 426, 435, 444, 453, 462, 477, 486, 495, 504 (15); `signature_nocgo.go:29:2` at lines 375, 384, 393, 402, 411, 420, 429, 438, 447, 456, 465, 480, 489, 498, 507 (15). They all fail trying to `mkdir /host-gomodcache/cache/download/github.com/decred` (`log:372-507`). The separate testcontainers error is at line 471: `internal/recovery/borrowed-transport-phase1_linux_test.go:31:2` cannot create `/host-gomodcache/cache/download/github.com/testcontainers/testcontainers-go/modules/postgres`.
- These failures map to the `FailedBuild` fields in `go-test.json:9-141`: Decred fails 15 package builds—`cmd/txharbor`, `internal/app`, `internal/app/reconcileadmin`, `internal/app/recoveryadmin`, `internal/config`, `internal/eth`, `internal/health`, `internal/indexer`, `internal/jointwire`, `internal/nonce`, `internal/reconciliation`, `internal/recovery/sources`, `internal/signer`, `internal/txlifecycle`, and `internal/withdrawal`. `internal/recovery` is the one testcontainers/Postgres-module failure (`go-test.json:100-105`). All 16 package failures are setup/build failures, not failed test assertions.
- The setup-go cache was a HIT for key `setup-go-Linux-x64-ubuntu24-go-1.26.5-c27b1067b11962675b4f5b29e737f4d3e6f959307900b295826198967794a17f`; 365,111,547 B (~348 MB) was restored (`log:148-157`). Host `GOMODCACHE` was `/home/runner/go/pkg/mod` (`log:189`); the run metadata records the container module-cache mount as read-only and `CGO_ENABLED=0` (`wrapper.IzHGk2/metadata.json:1`). Go environment lines show `GOFLAGS=''` (`log:183`), `GONOSUMDB=''` (`:191`), and `GOPROXY='https://proxy.golang.org,direct'` (`:195`).
- A full-log search found no `permission denied`, `no space left`, `dial tcp`, `network is unreachable`, `connection refused`, `no such host`, `proxyconnect`, or proxy-error/timeout matches. The observed dependency failure is the local read-only cache mkdir; the pinned Docker image was successfully pulled before tool checks (`log:300-368`). The `go: downloading` notices do not establish that either Go module was successfully fetched. **[INFERENCE]** The needed module contents had to be available in a writable/pre-populated cache before the container's read-only build phase; this log does not establish a Go-proxy/network outage.
- **Revision caveat:** the current checked-out wrapper has a `go mod download` preflight before fingerprinting (`scripts/drillcoverage/run-in-container.sh:37-53`), while the artifact identifies the run commit as `be272cb7da9f51e2af120cd116c4e2d2bbbefbf0` (`coverage.json:3`). The artifact does not include the wrapper source snapshot for that commit, so whether that current preflight existed in the failed-run revision is unestablished; compare the wrapper at the run SHA before attributing it to this run.
- **编排者注（消解 revision caveat）**：该 caveat 已由 git 证据消解——`git diff be272cb7da9f51e2af120cd116c4e2d2bbbefbf0..origin/main(2c1ccd33de9be76c0551b9596ce8e8f159f6eb83) -- scripts/drillcoverage/run-in-container.sh` 为空；失败版本 = `origin/main` 修复前 144 行 blob `d0d543bb1c851282ad7c74370dfc8885172aa691`（sha256 `ccadbec71a4f156344812ebc1822957579c3d50476f6b0b275dc9597c32b97f2`），其中**不含**任何 `go mod download` preflight（preflight 为本轮修复新增，+15 行）。详见 README §2「当前入口复检」。

## Test execution and coverage

- Nine packages emitted Go `Action: run` events, all followed by test/subtest `Action: pass`; the table counts individual test/subtest run/pass actions, not package summaries. The final package-level `Action: pass` is shown separately.

  |Package|Test/subtest run|Test/subtest pass|Package pass line|
  |---|---:|---:|---:|
  |`internal/cache`|17|17|685|
  |`internal/db`|18|18|295|
  |`internal/events`|98|98|644|
  |`internal/execution`|14|14|745|
  |`internal/logx`|13|13|804|
  |`internal/metrics`|45|45|991|
  |`internal/ratelimit`|16|16|1062|
  |`internal/recovery/controlstore`|35|35|1209|
  |`scripts/drillcoverage`|11|11|1263|

  These totals are 267 test/subtest `run` and 267 test/subtest `pass` events, plus nine package-level pass events. Source event ranges: cache `go-test.json:142-685`; db `:177-295`; events `:171-644`; execution `:686-745`; logx `:749-804`; metrics `:805-991`; ratelimit `:992-1062`; recovery/controlstore `:1063-1209`; drillcoverage `:1216-1263`.
- Five package-level `Action: skip` events mean "no test files": `internal/faultdrill` (line 748), `internal/perf` (808), `internal/recovery/controlstore/schema` (1124), `internal/testutil` (1212), and `migrations` (1215) (`go-test.json`). Sixteen package-level `Action: fail` events are the setup failures above. There were no failed test assertions in the packages that ran.
- **Required recovery drill tests never started: 0 test binaries from `internal/recovery` executed.** That package failed setup on the postgres testcontainers import (`go-test.json:100-105`). The separate `internal/recovery/controlstore` subpackage did run its 35 tests/subtests and pass; it is not the drill package.
- The checker defines 19 required test names and 3 required subtests (`scripts/drillcoverage/check.go:15-41`). The raw Go event artifact has no matching required-test run/pass events; `coverage.json:13-120` marks all 22 required cases `NOTRUN` with reason `missing from go test -json output`; `optional_skips` is empty (`coverage.json:121-124`).
- Result chain: `go_test_exit_status:1`, `checker_result:"FAIL"`, and `checker_exit_status:1` (`coverage.json:6-8`); checker/Make/workflow output is at log lines 1678-1681. The Docker CLI status is not separately printed. **[INFERENCE]** The wrapper propagated status 2 because it captures `docker_status` and exits with that value (`run-in-container.sh:148-159`) and the workflow observed exit 2 (`log:1681`).

## Summary

- The failure was a Go module-cache write error, not a Go test assertion: 15 packages hit Decred secp256k1 errors and `internal/recovery` hit the testcontainers Postgres module error (`log:372-507`; `go-test.json:9-141`).
- The test command was `go test -json -tags drill -count=1 -timeout 120m ./...` (`Makefile:88`); all 19 required tests plus 3 required subtests remained NOTRUN (`check.go:15-41`; `coverage.json:13-120`).
- 267 test/subtest events ran and passed across nine packages; 16 package setup failures and five no-test-file skips are itemized above (`go-test.json:9-1263`).
- Cache restored successfully (~348 MB) but module cache was read-only; `GOPROXY=https://proxy.golang.org,direct`, and searches found no proxy/network, permission, or disk-space errors (`log:148-195`; `metadata.json:1`).
- Tree validation passed with matching fingerprints (`wrapper.IzHGk2/tree-validation.json:1`); checker exited 1, Make failed, and the workflow step ended with code 2 (`coverage.json:6-8`; `log:1678-1681`).
- Current checkout includes a `go mod download` preflight, but the failed-run artifact has no source snapshot for commit `be272cb7...`; whether that code was present in the incident revision is unknown (`run-in-container.sh:37-53`; `coverage.json:3`). → 已由上述编排者注消解。
