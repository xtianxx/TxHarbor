# 015 独立 Recovery Drill runner 环境修复：只读模块缓存预物化（run 37298994567）

- 分支：`fix/drill-runner-modcache-prime`（自 `origin/main`
  `2c1ccd33de9be76c0551b9596ce8e8f159f6eb83` 新建；本地提交，**未推送/未合并**）｜
  修复对象：`scripts/drillcoverage/run-in-container.sh`（+15 行，单文件）
- 失败基线：Recovery Drill run `37298994567`（headSha
  `be272cb7da9f51e2af120cd116c4e2d2bbbefbf0`，event=schedule，FAIL）
- **Gate: T000-P 保持 OPEN**。本目录不是发布；不宣称独立 Drill 已在远程通过；
  生产 RPO/RTO/备份频率/保留未裁决（FR-036）。

## 0. 范围与结论

本轮只处理独立 Recovery Drill runner 在托管 runner 上的**环境性失败**：只读
`GOMODCACHE` + 缓存缺模块 → `make test-drill` 构建失败 → 必验场景全部 NOT RUN。
不触碰恢复业务语义、测试断言、必验集合、固定依赖版本与失败传播；不改变触发方式
（schedule/manual，不进普通 PR 门禁）。

1. 根因成立且可复现：setup-go 缓存命中（~348MB）但缺
   `github.com/decred/dcrd/dcrec/secp256k1/v4` 与
   `github.com/testcontainers/testcontainers-go/modules/postgres` 两个 go.mod 显式
   require 模块；容器以 `CGO_ENABLED=0` + `drill` tag 构建首次需要它们，只读挂载下
   `mkdir ...: read-only file system`，构建失败；22 项必验（19 顶层 + 3 子测试）
   0 启动，coverage checker 以 FAIL 阻止「部分绿 = 通过」。
2. 旧归因（013 证据页：「runner 环境性 read-only gomodcache，**保持单列，本轮不修**」）
   由本轮日志 + 装配复现证实，本目录为其闭合记录。
3. 修复 = `docker run` 前在宿主侧执行无参 `go mod download`（物化 go.mod 显式 require
   全集后再只读挂载）；失败即具名退出 1（fail-closed）。
4. 本地受控验证：修复前/后同构对照（缺模块 vs 恢复）、必验测试真实执行 PASS、
   失败传播 exit 1；完整 19+3 与 Kafka 事件层**留待准确 head 的远程运行**（§5）。

## 1. 失败基线（保留，不改写）

| 项 | 值 |
| --- | --- |
| run | `37298994567`（Recovery Drill，schedule，2026-10-05T10:49:13Z 起，job 1m19s） |
| headSha | `be272cb7da9f51e2af120cd116c4e2d2bbbefbf0`（当前 `origin/main` `2c1ccd3` 的父提交） |
| 失败步骤 | `full recovery drill layer (pinned native-tool runner)` → `scripts/drillcoverage/run-in-container.sh`；步骤 exit code 2 |
| artifact | `drill-evidence-37298994567`（id `11339964454`，zip sha256 `9d7373580d1ae829f6981ffedbd0ea869e1bdc34a37bed4d12687af1d3a8f9c3`，21915 B）——见 `run-37298994567/` |
| 旧记录（保留） | `docs/evidence/013/redis-latency-budget-design.md:421`（保持单列，本轮不修）；`docs/evidence/013/redis-latency-abc/README.md:177`（记录保留为待修项） |

时间线（行号 = 本地完整日志 `.evidence/drill-runner-env/logs/run-37298994567-full.log`，
摘录见 `run-37298994567/failure-excerpts.log`）：

1. `10:49:28Z` setup-go 缓存 **hit**：key `setup-go-Linux-x64-ubuntu24-go-1.26.5-c27b1067…`，
   ~348MB（行 150–157）；`GOMODCACHE=/home/runner/go/pkg/mod`（行 189）。
2. 容器装配探针**通过**：pg_restore/pg_dump 18.6 直连 ELF、`go version go1.26.5`、
   `docker info`、`go test ./scripts/drillcoverage` ok（行 365–368）。
3. `10:49:49.896Z/.910Z` `go` 开始下载两个模块（行 369/370）→ `10:50:29Z` 构建失败：
   - decred：`go-ethereum@v1.17.5/crypto/signature_nocgo.go:28:2`/`:29:2:
     mkdir /host-gomodcache/cache/download/github.com/decred: read-only file system`
     （行 372/375；全日志 30 行、覆盖 **15 个包**，行 372–507）→
     `cmd/txharbor`、`internal/app`、`internal/indexer`、`internal/signer` 等 15 包 `[setup failed]`；
   - postgres：`internal/recovery/borrowed-transport-phase1_linux_test.go:31:2:
     mkdir /host-gomodcache/cache/download/github.com/testcontainers/testcontainers-go/modules/postgres:
     read-only file system`（行 471）→ `internal/recovery [setup failed]`（行 474）。
4. coverage checker：`Drill scenario coverage incomplete`；22 项必验全部
   `NOT RUN (missing from go test -json output)`（行 1634+；同时 267 个测试/子测试在 9 个
   非必验包运行并全部 PASS——9 包 pass、16 包 setup fail、5 包 skip/无测试文件，失败均为
   setup/build 失败，无断言失败）；`coverage.json`：
   `go_test_exit_status=1`、`checker_result=FAIL`、`overall_result=FAIL`。
5. 退出链：`exit status 1`（1679）→ `make: *** [Makefile:76: test-drill] Error 1`（1680）
   → 步骤 `Process completed with exit code 2`（1681）。证据上传 `if: always()` 保留
   artifact；`tree-validation.json` 记录 `source_tree_stable=true`（指纹
   `sha256:cce2675f…`，与失败运行树一致）。

## 2. 根因（日志 + 装配 + 受控复现）

- **缺哪些模块、为什么缺**：两模块均为 go.mod 显式 require（`go.mod:29`、`go.mod:55`；
  go.sum 齐备）。decred secp256k1 仅出现在 `!cgo` 构建路径（go-ethereum `crypto`），
  postgres 模块仅被 `drill` tag 测试（`internal/recovery`）引用；runner 缓存来自从未
  做过 `CGO_ENABLED=0`/drill 构建的历史任务，因此从未抓取这两者；仓库内没有任何
  workflow/脚本执行 `go mod download` 预填充（grep 全工作流为空）。
- **失败为什么表现为只读 mkdir**：模块缺失 → go 需写
  `$GOMODCACHE/cache/download/...`（下载 + 解压 + per-`@v` 锁/`.info`/`.ziphash`）→
  容器内 `GOMODCACHE=/host-gomodcache` 只读 → `mkdir …: read-only file system`
  （`GOFLAGS=''`、`GOPROXY=https://proxy.golang.org,direct`，行 183/195；全日志无
  proxy/网络/权限/磁盘类错误匹配——失败点是缓存写入，与网络无关；`go: downloading`
  提示不构成抓取成功证据）。
- **必验测试是否实际启动**：对必验集**没有**。容器内 `go test -json -tags drill ./...`
  对可构建包仍在运行（9 个非必验包 267 个测试/子测试全部 PASS，含 `internal/cache`、
  `internal/db`、`internal/events`、`internal/recovery/controlstore` 等），但
  `internal/recovery` 与 15 个依赖 go-ethereum 的包 `[setup failed]` → 22 项必验 0 启动；
  checker 正确 FAIL。
- **当前入口复检**：`scripts/drillcoverage/run-in-container.sh` 在 `be272cb` 与当前
  `origin/main`（`2c1ccd3`）**逐字节相同**（该文件与 go.mod/go.sum 都未被 PR #41 改动；
  失败版本 = 144 行 blob `d0d543bb…`，sha256 `ccadbec7…`，**不含任何 priming**）
  → 问题在当前入口仍然存在；§4 的 V0a/V0b 以 `origin/main` blob 副本在同一条件下
  复现同签名。

## 3. 修复（最小环境修复，单文件 +15 行）

`scripts/drillcoverage/run-in-container.sh`，`cd "$REPO_ROOT"` 之后、`docker run` 之前：

```bash
if ! go mod download; then
	echo "could not prime the Go module cache for the containerized drill build" >&2
	exit 1
fi
```

- **无参 `go mod download` 的语义**（`go help mod download`，go ≥ 1.17）：
  “the modules needed to build and test the packages in the main module: the modules
  explicitly required by the main module …” 即 go.mod 显式 require 全集（含上述两模块）。
  实测无参下载**同时**物化 `cache/download` 与抽取目录；complete cache 下对只读挂载的
  容器构建零写入。
- **为什么不是其他方案**：只读挂载是既有加固（`metadata.json` 断言
  `module_cache: read-only`；本地历史 drill 依赖完整缓存离线运行）；改可写会把桥接缓存
  暴露给运行中进程并可能引入运行期下载。宿主侧预物化保持只读语义、可离线、失败可指名。
- **不变项**：Makefile、`check.go`/`check_test.go`、drill 测试与断言、必验集合、`go.mod`/
  `go.sum`、`drill.yml`（触发方式与门禁）零改动；priming 失败 = 具名非零退出，不降级、
  不跳过测试。
- **变更分级判断（真实契约对照）**：**环境修复级（runner 工具链装配）**。015 契约文本、
  FR/SC、恢复业务语义、必验清单、NOT RUN 纪律、树指纹校验与失败传播全部不变；
  无前提条件升级项。

## 4. 本地受控验证（隔离环境；完整 drill 本轮未跑）

披露：V0/V2 使用受测脚本副本，与受测文件的**唯一差异** = 末行 `exec make test-drill`
替换为有界命令（`go build ./...` + 完整 drill 测试二进制编译 + 单个必验测试真实执行），
逐字 diff 见 `local-verification/harness-diff-bounded-vs-tracked.txt` 与
`harness-diff-prefix-vs-origin-main.txt`。V3 单独披露：末行替换为**失败命令**
`exec sh -ec "go build ./internal/definitely-missing-package"`（验证非零退出传播），
逐字 diff 见 `local-verification/harness-diff-fail-vs-tracked.txt`。其余命令、挂载、镜像
（pinned `postgres@sha256:4ef4db…`）、uid/gid、环境变量与生产一致。

| 验证 | 条件 | 结果（证据） |
| --- | --- | --- |
| V1 冷启动 smoke | 空缓存（0B）→ 真实 `--smoke` | exit 0；0→716MB，两模块物化；20s（`v1-…log`） |
| V0a 负例 | **修复前**脚本 + 完全缺 decred | exit 1；`signature_nocgo.go:28:2/29:2 … read-only file system`（= 生产行 372/375 同签名）（`v0a-…log`） |
| V0b 负例 | **修复前**脚本 + 完全缺 postgres 模块 | exit 1；`borrowed-transport-phase1_linux_test.go:31:2 … read-only file system`；`internal/recovery [setup failed]`（= 生产行 471/474 同签名）（`v0b-…log`） |
| V2 正例 | **修复后**脚本 + 同一缺失状态 | exit 0；priming 恢复 → 容器构建 → drill 测试二进制编译 → 必验测试 `TestDrillTargetWitnessOldReconnectRejected` **真实执行 PASS**（5.57s；复跑 12.85s）（`v2-…log`） |
| V3 失败传播 | 修复后脚本 + 容器内失败命令 | exit 1（`v3-…log`） |

补强（独立执行，笔记见 `notes/env-audit.md`）：E1 在手工装配下复现两处生产签名（exit 1）；
E1c **仅**恢复这两个模块即容器内 build 与 drill 测试编译 exit 0（因果隔离到恰好这两个模块）；
E2 fresh cache 无参下载 9.97s/91 模块/0 错误，只读挂载下 `go build`、`go test -c` exit 0；
E4 `go list -deps -tags drill ./...`、`go vet -tags drill ./internal/recovery` 在只读缓存下
exit 0（全 drill-tag 依赖图无写入）。

写需求分类（与现状一致，摘要）：`/workspace`、`/host-go` 无需写；`/host-gomodcache`
仅缓存未命中时需要写（修复后零写）；`/drill-scratch`（GOCACHE/TMPDIR/HOME）与
`/drill-evidence` 必须可写；docker socket 仅 IPC。

验收入口检查（`notes/entry-acceptance.md`）：入口链（workflow → wrapper → make → guard →
`go test` → checker → 退出传播）逐项映射；4 组合成负/正例（空事件、缺必验、必验 SKIP、
全量必验 pass）exit 与报告均如期；NOT RUN 纪律、必验清单与包钉（`internal/recovery`）、
树指纹、证据唯一性、失败传播均有代码引用——环境修复未触碰任何一项。

受测树指纹（本地）：`sha256:1d89cc9328376ec4c9e15a510cb67ae7d3effd2440f0a660513ee6808fdbc5de`；
四组 wrapper 证据（V0b/V0a/V2/V3）`source_tree_stable=true`（运行前后一致）。

## 5. 已验 / 待验（不冒称独立 Drill 已通过）

已验（本地）：priming 正确性与充分性；只读挂载下完整构建与 drill 测试二进制编译；
单个必验测试真实执行 PASS；失败传播保持；checker/入口纪律（宿主侧）。

待验（远程，唯一验收口径）：在**包含本修复的准确 head** 上运行 `drill.yml`
（workflow_dispatch 或下一次 schedule；本轮不推送、不 dispatch），要求：步骤 exit 0、
`coverage.json` `overall_result=PASS`、19 必验 + 3 子测试 PASS、artifact 归档。
托管 runner 的 setup-go 缓存回写行为、完整 19+3 与 Kafka 事件层本地未复制，
不得据此宣称通过。

## 6. 归档物与哈希

- `run-37298994567/`：`failure-excerpts.log`（脱敏摘录，带行号）、`coverage.json`、
  `metadata.json`、`tree-validation.json`、`go-test.json.gz`（原始 249,211B → 19,502B，
  mtime=0）。
- `local-verification/`：V1/V0a/V0b/V2/V3 原始日志、受控序列整理
  （`sequence-results.md`）、harness diff。
- `notes/`：三路取证笔记（日志取证 / 环境核对 / 验收入口）。
- 完整本地日志（未入库，本地留存）：
  `.evidence/drill-runner-env/logs/run-37298994567-full.log`
  sha256 `fa9751f9b1b3f7663e3ede77b58b9218d1ef50fafa322b4e9609f91aed71730a`（1744 行）。
- 被测文件：修复前 `origin/main` blob `d0d543bb…`（sha256 `ccadbec7…`）；修复后 blob
  `022fa830…`（sha256 `af9955e4…`）。全部归档文件 sha256 见 `SHA256SUMS`。

## 7. 非声明与状态保持

- T000-P 保持 OPEN；无部署、无推送、无 PR、无 workflow_dispatch；生产预算/阈值/部署参数
  未改动；普通 PR 门禁不变（drill 独立通道）。
- 未重复 PR #41 已闭合测试；未运行完整 013/Kafka/性能矩阵；未改动恢复业务、测试断言与
  必验集合；旧失败历史（含 013 既有记录）原样保留。
- 下一步（另行授权）：推送后在准确 head 运行独立 Drill 并归档远程证据。

## 8. 独立复核记录（FinalDiffReview，未参与修改，2026-10-06）

- 复核面：修复最小性/正确性、无削弱（必验集合、NOT RUN、指纹、失败传播）、触发隔离、
  证据抽样、卫生（秘密/文件清单/`bash -n`/`diff --check`）、范围。运行时修复本身未发现问题。
- 首轮发现 2 项证据披露问题（均为文档层）：(1) §4 披露未区分 V3 的失败命令替换；
  (2) wrapper 证据组计数应为四组而非三组。
- 处置：新增 `harness-diff-fail-vs-tracked.txt`；§4 与 `sequence-results.md` 逐条修正
  （V0/V2 与 V3 分开披露；计数改为四组）；`SHA256SUMS` 重算。
