# 015 运行留存归档（2026-10-05）

候选树 `bb4f242`（`bb4f2422371fd273bbb80f81d079408e900d1536`，分支 `015-backup-recovery-safe-resumption`）发布前收口：
把 `/tmp/r2lab/` 下四个运行（drill5、pgfull3b、focusP11、focusP12）的留存按可复核方式归档到本目录。
本目录只新增文件，未重跑任何测试、未改动任何来源文件；全部为未跟踪证据（untracked，不 `git add`）。

- 宿主工具链：`go version go1.26.5 linux/amd64`（`/usr/local/go` 只读挂载进容器为 `/host-go`，容器内 `go version` 相同；模块缓存 `/home/dream/go/pkg/mod` 挂载为 `/host-gomodcache`，`GOCACHE` 指向容器内 `/pg-scratch/gocache`）。
- 容器镜像（pinned）：`postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`；pgfull3b 另用一次性本地镜像 `txharbor-dev/run-pg-git:pg18`（= pinned 镜像 + 安装 `git`，仅本地构建）。
- 时间口径：容器日志时间戳为 UTC，宿主文件时间为宿主本地时间（+08:00）。

## 1. 归档文件清单

| 归档文件 | 原名 | 处理方式 | 字节 | 行数 | sha256 | 来源路径 | 运行树（提交） |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `pgfull3b_pg-integration.jsonl.gz` | `pg-integration.jsonl` | gzip（`-9 -n`，无损；原始 5,845,370 B / 23,569 行） | 448,846 | 23,569（原始 jsonl 行数） | `fb1bed303b8aa77eb03c05268d2353594939402be16ad58a375cd58ae0bcae45` | `/tmp/r2lab/run_pgfull3b/pg-integration.jsonl` | `1a6779f` |
| `pgfull3b_summary.json` | （派生） | summary（从 jsonl 的 `Action` 事件生成：顶层/子测试 pass/fail/skip、SKIP 名、TestDrillCLI* 9 项、*MigrationHistoryUntouched 3 项、9 个 VCS 修复项状态） | 9,161 | 229 | `095c336d85533045052830755e67b8afc2379973b7452424c88b38202fbeab7f` | 同上（派生） | `1a6779f` |
| `pgfull3b_repo-root.txt` | `repo-root.txt` | full（逐字节拷贝；运行自记 `/workspace` + `1a6779f …`） | 270 | 2 | `b901434a79d2d3df89ae9bb32a871846201bd03750a4d5366a139cc5a12cb543` | `/tmp/r2lab/run_pgfull3b/repo-root.txt` | `1a6779f` |
| `focus_head_tail.txt` | `focus.log` | trimmed（头 50 行 + 尾 200 行；中间 1 行 `[... 5664 lines elided ...]`，头部注明 total_lines=5914） | 23,858 | 253 | `9928154effa382284f1470e8fad295a657e4df63f5aa37d4c662690b34b58cc6` | `/tmp/r2lab/run_drill5/focus.log` | `678b8bd` |
| `drill5_required_pass.txt` | `focus.log` | trimmed（19 项必验顶层 + 3 项必验子测试的原始 `--- PASS` 行，逐行，共 22 行；清单取自 `scripts/drillcoverage/check.go` 的 `requiredTests`/`requiredSubtests`） | 1,634 | 22 | `5adeec8b49a8d8c222a6d8b1d83f3950cd352d2ae1521f070d880bf87d3323d7` | `/tmp/r2lab/run_drill5/focus.log` | `678b8bd` |
| `focusP11_focus.log` | `focus.log` | full（逐字节拷贝，sha256/md5 与来源一致） | 25,383 | 197 | `90fb5d88804207c15eee88d7de6afdb0f1953c2b663f8e63591704114ec3366c` | `/tmp/r2lab/run_focusP11/focus.log` | `bb4f242` |
| `focusP12_focus.log` | `focus.log` | full（逐字节拷贝，sha256/md5 与来源一致） | 112,214 | 844 | `0137030795eec43c41cd2631d58ba239c6b63af1aafb1c1aae80126f0605ae53` | `/tmp/r2lab/run_focusP12/focus.log` | `bb4f242` |

行数口径：文本文件为 `wc -l`；gzip 为二进制，记原始 jsonl 行数（`gzip -dc … | wc -l`）。

## 2. 来源校验（原始文件）

| 来源路径 | 字节 | 行数 | sha256 | md5 | 与归档的关系 |
| --- | --- | --- | --- | --- | --- |
| `/tmp/r2lab/run_pgfull3b/pg-integration.jsonl` | 5,845,370 | 23,569 | `47cf14c062fb48339dc634f625e9431fc75426591e3350c0f01ee5419c95262b` | `d2422951728d43d82ffe4765a3dee1b9` | gzip 往返一致：`gzip -dc pgfull3b_pg-integration.jsonl.gz \| sha256sum` 等于本行 sha256 |
| `/tmp/r2lab/run_pgfull3b/repo-root.txt` | 270 | 2 | `b901434a79d2d3df89ae9bb32a871846201bd03750a4d5366a139cc5a12cb543` | `0b7f5afd0840621437616af28c2b7090` | 与 `pgfull3b_repo-root.txt` 逐字节相同 |
| `/tmp/r2lab/run_drill5/focus.log` | 665,962 | 5,914 | `2a4bdecc749764a64d4aa7b18293053f7b8dc87751be76219a2405566f8757db` | `b053fe2b8498b6939569ad61da2d86a6` | 派生出 `focus_head_tail.txt` 与 `drill5_required_pass.txt`（行内容逐行取自本源） |
| `/tmp/r2lab/run_focusP11/focus.log` | 25,383 | 197 | `90fb5d88804207c15eee88d7de6afdb0f1953c2b663f8e63591704114ec3366c` | `e9ea2d07269de35935fd3374688e5e9d` | 与 `focusP11_focus.log` 逐字节相同（sha256/md5/行数一致） |
| `/tmp/r2lab/run_focusP12/focus.log` | 112,214 | 844 | `0137030795eec43c41cd2631d58ba239c6b63af1aafb1c1aae80126f0605ae53` | `6ea6fe42fcfe0d0327d3839f413a1130` | 与 `focusP12_focus.log` 逐字节相同（sha256/md5/行数一致） |

## 3. 运行环境与命令

### 3.1 docker run 参数模板（gold pattern，`/tmp/r2lab/run_suite.sh`）

```bash
ROOT=/home/dream/product_env/TxHarbor
RUN_DIR=/tmp/r2lab/run_<LABEL>; SCRATCH=$RUN_DIR/scratch   # scratch 内建 gocache/tmp/home
docker run --rm --init --network host --user 0:0 --group-add 989 \
  --mount "type=bind,src=$ROOT,dst=/workspace,readonly" \
  --mount "type=bind,src=$ROOT,dst=$ROOT,readonly" \
  --mount "type=bind,src=/usr/local/go,dst=/host-go,readonly" \
  --mount "type=bind,src=/home/dream/go/pkg/mod,dst=/host-gomodcache,readonly" \
  --mount "type=bind,src=/usr/bin/docker,dst=/usr/bin/docker,readonly" \
  --mount "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock" \
  --mount "type=bind,src=$RUN_DIR,dst=/pg-evidence" \
  --mount "type=bind,src=$SCRATCH,dst=/pg-scratch" \
  -e 'PATH=/usr/lib/postgresql/18/bin:/host-go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
  -e 'GOROOT=/host-go' -e 'GOMODCACHE=/host-gomodcache' -e 'CGO_ENABLED=0' \
  -e 'GOCACHE=/pg-scratch/gocache' -e 'TMPDIR=/pg-scratch/tmp' -e 'HOME=/pg-scratch/home' \
  -e 'DOCKER_HOST=unix:///var/run/docker.sock' \
  -e 'TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1' -e 'TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock' \
  -e 'CI=true' -e 'TXHARBOR_REQUIRE_DOCKER=1' \
  --entrypoint /bin/sh 'postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280' -ec '<suite 命令>'
```

- 关键点：pinned 镜像、`--user 0:0`、`--network host`（sibling testcontainers 的 `127.0.0.1` 端口发布可达）、仓库只读挂载 `/workspace`、Go 工具链/`GOMODCACHE`/`GOCACHE` 挂载。
- pgfull3b 变体：镜像换为 `txharbor-dev/run-pg-git:pg18`；suite 命令前先
  `export GIT_CONFIG_GLOBAL=/pg-scratch/home/.gitconfig && git config --global --add safe.directory /workspace`，
  再执行 `git rev-parse --show-toplevel` + `git log --oneline -1` 写入 `/pg-evidence/repo-root.txt`（即 `pgfull3b_repo-root.txt`）。

### 3.2 go test 命令行

| 运行 | 命令 |
| --- | --- |
| drill5 | `go test -tags "linux,drill" -count=1 -timeout 120m -v ./internal/recovery/`（全量 296 顶层组；`RUN_RE=ALL`） |
| pgfull3b | `go test -json -tags integration -count=1 -timeout 60m ./... > /pg-evidence/pg-integration.jsonl`（`go_test_exit=0`，`runner_exit=0`） |
| focusP11 | `go test -tags "linux,drill" -count=1 -run '^TestBorrowedReplacementBoundPostcommitNativeStart$' -v ./internal/recovery/` |
| focusP12 | `go test -tags "linux,drill" -count=1 -run '^TestBorrowedReplacementBound(BaselineCommit\|PostcommitAdmission\|ReentryExclusion)$' -v ./internal/recovery/` |

focus 定向运行即 `docker run … go test -tags "linux,drill" -run REGEX` 形式；上表 P11/P12 的 `-run` 正则为按日志中实际执行的顶层测试集合复现的等价命令（一次性 runner 脚本未留存命令回执）。

### 3.3 统计口径（顶层 vs 子测试）

- 文本日志（drill5 / focusP11 / focusP12）：
  - 顶层终态行：`grep -cE '^--- (PASS|FAIL|SKIP):'`；子测试终态行：`grep -cE '^[[:space:]]+--- (PASS|FAIL|SKIP):'`。
  - 判定：测试名含 `/` = 子测试，不含 `/` = 顶层；包级输出行（`ok`/`FAIL <pkg>`）不计入。
- pgfull3b jsonl（`go test -json`）：
  - 只统计 `Action ∈ {pass, fail, skip}` 且 `Test` 非空的事件；每个 `(Package, Test)` 恰好一个终态事件（已做重复断言，重复数 0）。
  - `Test` 含 `/` = 子测试，否则 = 顶层；`Test` 为空的包级 `skip`（`[no test files]`）不计。
  - 摘要脚本口径见 `pgfull3b_summary.json` 的 `methodology` 字段。

## 4. 关键计数（与来源日志核对）

### drill5（来源 `focus.log`，5,914 行）

- 顶层 296 组：**295 PASS / 0 FAIL / 1 SKIP**（SKIP 为 `TestBorrowedAuthEntryProbeFDChildProcess`，wrapper 自引用 helper，历来如此）；子测试 **343 PASS / 0 FAIL / 0 SKIP**。
- 必验 19 顶层 + 3 子测试（`scripts/drillcoverage/check.go` 的 `requiredTests`/`requiredSubtests`）全部 PASS，逐行见 `drill5_required_pass.txt`（22 行）。
- 必验集合同步（2026-10-05，CI run 37252107877 定向修复）：`TestTargetWriterProductionExecutableDiscovery` 的构建标签由 `linux` 改为 `integration && linux`（真实 pg_restore/可执行拓扑验证归 integration 通道；unit 通道不得依赖 runner PATH 上偶然存在的 pg_restore），drill 必验以 drill5 已 PASS 的等价可执行拓扑断言 `TestDrillArmRefusesReplacedELFAndPreStartTamper`（来源 `focus.log` 第 5912 行 `--- PASS:`）等价替换；替换后 `requiredTests` 仍为 19 项顶层 + 3 项必验子测试，断言零删减。`drill5_required_pass.txt` 为替换前那次运行的历史记录，按原样保留、不追溯改写。

### pgfull3b（来源 `pg-integration.jsonl`；HEAD `1a6779f`；`go_test_exit=0`）

- 顶层 1825：**1822 PASS / 0 FAIL / 3 SKIP**；子测试 **2082 PASS / 0 FAIL / 0 SKIP**。
- 3 SKIP（均非必验 helper）：`TestCrashHelper`（internal/txlifecycle）、`TestWithdrawalIntakeStorageDown`（internal/withdrawal）、`TestEpochIONonRootHelperProcess`（internal/recovery）。
- `TestDrillCLI*` 9 项（7 顶层 + 2 子测试）全部 `pass`；`Test*MigrationHistoryUntouched` 3 项全部 `pass`；pgfull3 首跑 9 个 `error obtaining VCS status` 失败项全部 `pass`；首跑其余 4 个 FAIL（3 个 migration-history git-128 + 1 个 production CLI build `exit status 1`）全部 `pass`。
- 逐项状态与首跑错误见 `pgfull3b_summary.json`（`test_drill_cli_items` / `migration_history_untouched_items` / `vcs_repair_items` / `other_first_pass_failures_closed`）。

### focusP11（来源 `focusP11_focus.log`）

- 顶层 1 PASS / 0 FAIL / 0 SKIP：`TestBorrowedReplacementBoundPostcommitNativeStart`（180.6s）；子测试 11 PASS（`P` + `N1`..`N10`）。

### focusP12（来源 `focusP12_focus.log`）

- 顶层 3 PASS / 0 FAIL / 0 SKIP：`TestBorrowedReplacementBoundBaselineCommit`、`TestBorrowedReplacementBoundPostcommitAdmission`、`TestBorrowedReplacementBoundReentryExclusion`（合计 764.7s）；子测试 24 PASS。

## 5. 运行树对应说明

四个运行挂载的都是宿主工作树（只读），下表"运行树"为运行当时的 HEAD（pgfull3b 以运行自记 `repo-root.txt` 为准）：

- **pgfull3b → `1a6779f`**（`1a6779f590ebf86770c186decc50720dc9f3af51`）。其后到候选 HEAD `bb4f242` 的提交（`68ebdc7`/`6869eba`/`f1cecfa` 证据与任务文档、`28a224f` .gitignore、`e7f029e` 收纳 92 个 `.go`（"no edits, contents unchanged"）、`bb4f242` runner 脚本 + .dockerignore）不含被测逻辑变更。
- **drill5 → `678b8bd`**（`678b8bd499eb79a76e6be3fb3a487801ae0c31f0`）：运行开始于 2026-10-04 23:08:17 +08:00，紧随 `678b8bd`（23:07:40，P lane 计数规则）之后；之后的 `ef1432c` 等为证据文档。
- **focusP11 → `bb4f242`**：开始于 2026-10-05 03:23:01 +08:00，紧随 HEAD 提交（03:22:32）之后。
- **focusP12 → `bb4f242`**：开始于 2026-10-05 03:26:48 +08:00。
- 说明：`e7f029e` 收敛的 92 个 `.go` 文件在以上运行当时已存在于工作树（该提交注明 "these files were exercised by drill5/pgfull3b/focus runs; no edits, contents unchanged"），因此四个运行覆盖的测试源与候选 HEAD `bb4f242` 一致。

## 6. 未归档内容与边界

- scratch 未归档：`run_pgfull3b/scratch/gocache` 695 MB、`run_drill5/scratch/gocache` 464 MB（均为可重建编译缓存），`scratch/home` 仅 go telemetry 计数、`scratch/tmp` 为空；按"仅当小"条件不纳入。focusP11/P12 的 scratch 同构。
- 未触碰 `docs/evidence/015/` 下既有 124 个未跟踪目录；本目录未 `git add`/`commit`；`scripts/pgintegration/run-pg-integration.sh` 仅头部注释新增（零执行逻辑改动）。

## 7. 复核命令（sha256）

```bash
cd docs/evidence/015/verification-archive-2026-10-05
sha256sum -c <<'EOF'
fb1bed303b8aa77eb03c05268d2353594939402be16ad58a375cd58ae0bcae45  pgfull3b_pg-integration.jsonl.gz
095c336d85533045052830755e67b8afc2379973b7452424c88b38202fbeab7f  pgfull3b_summary.json
b901434a79d2d3df89ae9bb32a871846201bd03750a4d5366a139cc5a12cb543  pgfull3b_repo-root.txt
9928154effa382284f1470e8fad295a657e4df63f5aa37d4c662690b34b58cc6  focus_head_tail.txt
5adeec8b49a8d8c222a6d8b1d83f3950cd352d2ae1521f070d880bf87d3323d7  drill5_required_pass.txt
90fb5d88804207c15eee88d7de6afdb0f1953c2b663f8e63591704114ec3366c  focusP11_focus.log
0137030795eec43c41cd2631d58ba239c6b63af1aafb1c1aae80126f0605ae53  focusP12_focus.log
EOF
gzip -dc pgfull3b_pg-integration.jsonl.gz | sha256sum   # → 47cf14c062fb48339dc634f625e9431fc75426591e3350c0f01ee5419c95262b
```
