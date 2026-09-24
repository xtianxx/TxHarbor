# 013 CI 耗时基线与预算（T084）

- Feature: `013-reliable-event-infrastructure`；批次: B11（T084）；基准 HEAD: `65b2f07` + B11 改动（CI/文档/审计）。
- 口径（必须随引用携带）：**下表为本地测量**（本仓库开发主机，非 GitHub 目标 runner）。
  目标 runner（`ubuntu-latest`）基线**待核验**（未推送/未触发远程 CI）；预算按公式由本地
  基线推导并同步到 CI 配置，远程首跑后必须用远程实测复核——若远程基线超过预算，
  MUST 以远程实测重新推导预算（不得静默放宽、不得编造）。
- 测量命令: `make lint` / `make build` / `make test` / `make test-race` / `make test-contract` /
  `make test-integration` / `make test-integration-redis` / `make test-integration-kafka` /
  `make test-e2e`；独立层 `make test-fault` / `make test-perf`（B10 记录）。
- 原始日志（未入库）: `/tmp/opencode/b11_ci_budget/*.log`；B10 独立层: `/tmp/opencode/b10_fault_final.log`、`b10_perf_final.log`。

## 1. 环境规格（本地测量）

| 项 | 值 |
|---|---|
| 主机 | DESKTOP-TXT123（WSL2, linux/amd64） |
| CPU / 内存 | AMD Ryzen 7 5800H（16 核）/ 15915 MiB |
| Go | go1.26.5 |
| Docker | 29.6.1（testcontainers v0.44.0，镜像已本地缓存：`postgres:18.6-trixie`、`ghcr.io/foundry-rs/foundry:v1.8.1`、`redis:8.2.10-alpine`、`confluentinc/confluent-local:7.9.10`） |
| 备注 | 本地镜像已预热；目标 runner 首跑含镜像拉取，属预算余量覆盖范围 |

## 2. 各层本地基线（wall clock，串行单次）

| 层 | 第一次测量 | 干净复跑 | 备注 |
|---|---|---|---|
| lint（gofmt + vet） | 3s | 2s（final-lint） | 纯 Go |
| build | 4s | 4s（final-build） | 纯 Go |
| unit（`make test`，含 T085 审计） | 8s | 9s（unit-final）/ 8s（final-test） | 纯 Go，无中间件 |
| race（`make test-race`） | 9s | 11–17s | 纯 Go；见 §4 抖动记录 |
| contract（`make test-contract`） | 9s | 7s（contract-final） | 纯 Go，无 Docker |
| integration-redis | 49s（当次失败：既有测试时序抖动，见 §4） | **50s（绿）** | Redis 容器 |
| integration-kafka | 184s | — | Kafka 容器 |
| e2e（`make test-e2e`） | 70s（当次受中间态编译污染，见 §4） | **67s（绿）** | 全栈 + Anvil |
| integration-pg（`make test-integration`） | 311s | — | PostgreSQL 容器（最大层） |
| fault（独立层，B10 记录） | `internal/faultdrill` 751.6s ≈ 12.5m | — | 五态矩阵/演练；不进普通 PR |
| perf（独立层，B10 记录） | `internal/perf` 368.5s ≈ 6.1m（`TXHARBOR_PERF_REPEATS=2`） | — | V-BENCH；不进普通 PR |

## 3. 预算公式与同步结果

**公式**：`预算 = max(ceil(基线 × 余量系数), 固定下限)`，且 `预算 ≤ 该层 CI timeout 硬上限`。

**余量系数与下限来源**（显式，不编造）：

- 余量系数 **2.0**：纯 Go 层（lint/build/unit/race/contract）——目标 runner 为 4 vCPU、
  冷构建缓存，且 race 为 CPU 敏感；本地基线极小，系数影响有限。
- 余量系数 **3.0**：容器层（integration-pg/redis/kafka/e2e）与独立层（fault/perf）——
  目标 runner 4 vCPU（本地 16 核）、镜像拉取、容器冷启动、磁盘吞吐差异。
- 固定下限 **5m**：纯 Go 步骤（覆盖 checkout/setup-go/缓存恢复的固定开销）。
- 固定下限 **10m**：容器层步骤（覆盖 Docker 校验与容器启动固定开销）。
- 固定下限 **30m**：独立 Fault/Perf 步骤（长场景固定等待与容器反复启停）。
- timeout 硬上限 = workflow 中 job/step 现有上限（lint/build 10m、unit job 25m、
  contract 10m、pg job 40m、redis job 30m、kafka/e2e job 40m、fault/perf job 90m）。

| 层/步骤 | 基线（本地） | 余量 | 下限 | 预算 | 硬上限 | 同步到 CI 的 timeout |
|---|---|---|---|---|---|---|
| lint step | 3s | ×2 | 5m | 5m | 10m | 5m |
| build step | 4s | ×2 | 5m | 5m | 10m | 5m |
| unit step | 9s | ×2 | 5m | 5m | 10m（job 25m） | 5m |
| race step | 17s | ×2 | 10m | 10m | 15m（job 25m） | 10m |
| contract step | 9s | ×2 | 5m | 5m | 10m | 5m |
| integration-pg step | 311s | ×3 | 10m | 16m | 20m（job 40m） | 16m |
| integration-redis step | 50s | ×3 | 10m | 10m | 20m（job 30m） | 10m |
| integration-kafka step | 184s | ×3 | 10m | 10m | 25m（job 40m） | 10m |
| e2e step | 67s | ×3 | 10m | 10m | 25m（job 40m） | 10m |
| changes job | 秒级 | ×2 | 5m | 5m | — | 5m |
| ci-required gate | 秒级 | ×2 | 5m | 5m | — | 5m |
| fault step（独立） | 751.6s | ×3 | 30m | 38m → 取整 45m | 60m（job 90m） | 45m |
| perf step（独立） | 368.5s | ×3 | 30m | 30m | 60m（job 90m） | 30m |

说明：fault 预算 38m 向上取整到 45m（5 分钟粒度），为远程 4 vCPU 下的场景等待留余量；
perf 预算 30m 即下限。全部预算 ≤ 硬上限。

## 4. 测量异常与失败证据（不得掩盖）

| 次 | 层 | 结果 | 证据 | 处置/结论 |
|---|---|---|---|---|
| B11 测量第一次 | integration-redis | `rc=2`，`TestClientFallbackSingleflightCollapsesSameKey`（`internal/cache`，Unit 载体/fake store）报 `loader calls = 11, want 1` | `integration-redis.log` | 既有测试时序抖动；干净复跑 **绿**（`integration-redis-rerun.log`，50s） |
| B11 测量第一次 | e2e | `rc=2`，失败为 T085 审计测试的**中间修订版**（测量运行编译了编辑中的文件） | `e2e.log` | 测量污染，非产品失败；干净复跑 **绿**（`e2e-rerun.log`，67s） |
| B11 复跑 | race | `rc=2`，同一 `TestClientFallbackSingleflightCollapsesSameKey` 报 `loader calls = 2` | `race-final.log`、`race-rerun2.log`、`race_probe_2.log` | 见下风险登记 |
| B11 最终态复跑 | unit | `rc=2`，同一测试报 `loader calls = 13` | `final-test2.log`、`unit_probe_6.log`（失败样本） | 见下风险登记；同树多次复跑绿（`unit-final.log`、`final-test.log`、`unit_probe_1..5.log`） |
| — | — | 其余最终检查 | `final-lint.log`/`final-build.log`/`final-test.log`/`unit-final.log`/`contract-final.log` | 绿 |

**风险登记（未闭合，交 orchestrator 裁决）**：`TestClientFallbackSingleflightCollapsesSameKey`
（`internal/cache/cache_test.go:268`，B7/T059 既有交付物）在 B11 观测中偶发失败——
失败样本：隔离探针 12 次 1 红（`loader calls = 4`）；全量 `make test-race` 6 次 3 红
（`race-final.log`/`race-rerun2.log`/`race_probe_2.log`）；全量 `make test` 3 次红
（有日志样本 `final-test2.log`/`unit_probe_6.log`，另 1 次为组合校验未落盘；其余
`unit-final`/`final-test`/`unit_probe_1..5` 绿）；Integration-Redis 首次 1 红
（`loader calls = 11`）。失败全部为 loader 调用数 > 1，**无数据竞争、无门禁/资金断言
失败**；同树多次复跑可绿。
机制分析：单飞（singleflight）只合并**时间重叠**的调用；该测试等待「首个 loader 进入后」
放行，再断言 16 个并发调用恰好合并为 1 次——若个别 goroutine 在 leader 返回后才进入
单飞，则产生第 2 次加载（实现语义正确，断言窗口不受同步保证）。该测试为 B7 既有交付物，
B11 授权范围不允许修改 cache 代码/测试，故未修复；建议后续单独任务加固（例如：为全部
16 个调用建立确定性的「已进入单飞」屏障后再放行，或以断言「≤ 并发上限」替代「恰好 1」
并单独验证串行路径）。在此之前：unit/race 层偶发红**不得**读作产品回归，也不得据此
宣称门禁/资金安全回归。

## 5. 远程核验清单（待核验）

- [ ] 目标 runner 各层基线（冷缓存、镜像拉取）实测并回填本文件 §2（本地列保留）。
- [ ] 用远程实测按 §3 公式复核预算；超预算时以远程实测重推导并更新 CI 配置。
- [ ] 分支保护必需检查名称与 workflow 作业名对齐（`lint (gofmt + vet)`、`build`、
  `unit tests (+ race)`、`contract tests (no Docker)`、`ci-required`）。
- [ ] `ci:integration-pending` 流程在真实缺 Docker 场景下的演练记录。
- [ ] fault-perf 手动触发一次并留存 artifact（证据目录）。
