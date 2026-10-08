# TxHarbor 验证矩阵与证据索引

> **事实边界**：本文件汇总**已留存**的验证证据（本地归档 + 远端 CI），并区分口径：
> 本地 / 远端 CI / 生产就绪三者分离，不得混同。项目未部署、T000-P 保持 OPEN，
> 不宣称生产就绪或生产 SLO。
> **本次文档整理未执行任何测试层**（未启动服务/迁移/测试/基准/演练）；下表中「状态」均指
> **已留存证据所记录的状态**，对当前工作树一律标注 NOT VERIFIED。

## 1. 状态口径（先读）

| 标记 | 含义 |
| --- | --- |
| **PASS** | 有留存证据（测试文件 + 归档报告 + 运行记录/CI run）证明该层在该树/该运行上通过 |
| **FAIL** | 有留存证据证明失败（含原因未确认者；不得用「重试通过」掩盖） |
| **NOT RUN** | 该层在该次运行中未执行（如路径分类跳过、缺 Docker、缺工具）；**NOT RUN 不得读作通过** |
| **NOT VERIFIED** | 无留存证据，或本文件整理时未复跑；不代表通过或失败 |

## 2. 分层验证矩阵

| 层 | 入口（`Makefile` / 标签） | Docker | 证明什么 | 已留存状态 |
| --- | --- | --- | --- | --- |
| Unit | `make test`（无标签） | 否 | 纯逻辑/契约边界 | **PASS**（远端：PR #44 `37745833563`、PR #45 `37769238098`、main `37771281932` 的 unit 作业） |
| Race | `make test-race` | 否 | 并发安全（race detector） | **PASS**（同上运行，unit(+race) 作业） |
| Contract | `make test-contract`（`contract`） | 否 | 事件信封/目录/schema 版本/消费者兼容 + 014/015 契约 | **PASS**（同上运行，contract 作业） |
| Integration PostgreSQL | `make test-integration`（`integration`） | 是 | 真实 PG + 真实迁移下的存储/并发/恢复语义 | **PASS**（远端：PR #44；main `37748375307` attempt 2 的 PG 作业 24 包全 ok） |
| Integration Redis | `make test-integration-redis`（`integration_redis`） | 是 | 缓存回源/失效、限流失效处置（PD-1） | **PASS（远端，历史）**：main `1e09c61` 的 run [`37706916514`](https://github.com/xtianxx/TxHarbor/actions/runs/37706916514)（Redis 作业 success）；§3 中除该 run 外的其余四次运行该层路径跳过；本地归档见 `docs/evidence/013/`（Q5/Q6） |
| Integration Kafka | `make test-integration-kafka`（`integration_kafka`） | 是 | 发布器崩溃/重复/多实例、消费幂等 | **PASS**（远端：PR #44；main attempt 2 的 Kafka 作业） |
| E2E | `make test-e2e`（`e2e`） | 是 | 核心充提全链（全栈 + Anvil） | **PASS（远端，历史）**：run [`37706916514`](https://github.com/xtianxx/TxHarbor/actions/runs/37706916514)（e2e 作业 success）；§3 中除该 run 外的其余四次运行该层路径跳过；本地记录见 `docs/evidence/013/`、`docs/evidence/015/README.md`（日志在 `/tmp`，未入库） |
| Fault Injection | `make test-fault`（`fault`） | 是 | 五态故障矩阵与不变量 | **PASS（本地归档 + 远端历史）**：本地 `docs/evidence/013/quickstart_evidence_index.md` Q8/Q9、`internal/faultdrill/`；远端 run [`37297533285`](https://github.com/xtianxx/TxHarbor/actions/runs/37297533285) 的 fault injection (V-DRILL) success |
| Recovery Drill | `make test-drill`（`drill`，独立通道） | 是 + 恢复工具 | S1–S12 + F1–F7 全流程与负例 | 见 §4（含失败披露与后续运行） |
| Performance Diagnostics | `make test-perf`（`perf`，独立层） | 是 | PG-only vs 全栈对照；分段测量 | **PASS（测量记录 + 远端历史）**：本地 `docs/evidence/013/benchmark_report.md`、`docs/evidence/013-supplement/consumer-seg/`、`docs/evidence/019-normal-query-seg/`；远端 run `37297533285` performance success、run `37643963489` performance success（产物归档 `.evidence/ci-runs/perf-artifact-37643963489/`） |

## 3. 已留存远端 CI 运行（选列）

| Run | 事件 / 分支 | Head | 结果 | 说明 |
| --- | --- | --- | --- | --- |
| [`37706916514`](https://github.com/xtianxx/TxHarbor/actions/runs/37706916514) | push / main | `1e09c61` | **success** | lint/build/unit(+race)/contract/PG/**Redis**/**e2e** 通过；Kafka 路径分类跳过（本地日志留存 `.evidence/ci-runs/37706916514.log`） |
| [`37745833563`](https://github.com/xtianxx/TxHarbor/actions/runs/37745833563) | pull_request / `perf/consumer-catchup-seg`（PR #44） | `15e3491` | **success** | lint/build/unit(+race)/contract/PG/Kafka 通过；Redis、e2e 路径分类跳过 |
| [`37748375307`](https://github.com/xtianxx/TxHarbor/actions/runs/37748375307) | push / main | `bb8a56c` | attempt 1 **FAIL** → attempt 2 **success** | 首次失败在 PostgreSQL 层：`TestRestoreInterruptionNotRestoredAndRerunIdempotent`（`target still has 1 sessions`）；**原因未确认**，同 SHA 单次重试通过不构成根因修复结论 |
| [`37769238098`](https://github.com/xtianxx/TxHarbor/actions/runs/37769238098) | pull_request / `docs/portfolio-showcase`（PR #45） | `bb2958a`（合成 merge `aa60ed3`） | **success** | 纯文档变更：lint/build/unit(+race)/contract 通过；PG/Redis/Kafka/e2e 路径分类跳过 |
| [`37771281932`](https://github.com/xtianxx/TxHarbor/actions/runs/37771281932) | push / main | `6f9003a1`（PR #45 合并提交） | **success** | 同上（纯文档变更，四个中间件层路径跳过） |

运行原始日志与元数据（含 main run `37748375307` 的失败 attempt 1 与成功 attempt 2；本地 git-ignored）：
`.evidence/ci-runs/verify/013-consumer-seg-pr44/`
（attempt 1 与 attempt 2 分开留存）、`.evidence/ci-runs/verify/docs-portfolio-showcase/`。

## 4. Recovery Drill 状态（含失败披露）

| 时间 / 载体 | 结果 | 说明 |
| --- | --- | --- |
| 本地归档 `docs/evidence/015/quickstart_matrix_evidence.md` | **FAIL（3 项必验）** | 必验 19 PASS / 3 FAIL：S4 restore、F2 中断/部分恢复、F4 外部事实领先（Kafka committed-offset 目标守卫未清洁）；顶层 913 PASS / 3 FAIL |
| 本地归档 `docs/evidence/015/README.md` | 快照 | 全量 PG 集成 1755 PASS / 8 FAIL / 2 SKIP（exit 1）；后续聚焦修复 3 项 PASS，**不改变全量失败结论**；任务台账（该快照）64/70、当时六项 OPEN（T019/T020/T057/T058/T066/T068；后续本地收口见 `specs/015-backup-recovery-safe-resumption/tasks.md`），生产前置与 T000-P 仍保持 OPEN |
| 本地归档 `docs/evidence/015/verification-archive-2026-10-05/` | **PASS（后续运行）** | drill5：顶层 295 PASS / 0 FAIL / 1 SKIP（既有 helper SKIP）；**必验 19 顶层 + 3 子测试全部 PASS**（`drill5_required_pass.txt`，22 行）；pgfull3b：1822 PASS / 0 FAIL / 3 SKIP，`go_test_exit=0`。注：2026-10-05 有一次必验集合同步（CI run 37252107877），一项必验以等价可执行拓扑断言替换，断言零删减；`drill5_required_pass.txt` 为替换前历史记录，按原样保留 |
| 远端 Drill run [`37298994567`](https://github.com/xtianxx/TxHarbor/actions/runs/37298994567)（schedule，2026-10-05，head `be272cb`） | **FAIL（环境性）** | 只读 `GOMODCACHE` 缺模块 → `make test-drill` 构建失败 → 必验场景全部 NOT RUN；修复（模块缓存 priming）随后合入 main（`7587116`）；修复分支的重跑证据见下一行 |
| 远端 Drill run [`37576884648`](https://github.com/xtianxx/TxHarbor/actions/runs/37576884648)（workflow_dispatch，2026-10-07，head `f4fab73`，分支 `fix/drill-runner-modcache-prime`） | **PASS（修复分支）** | 修复后必验 **22/22 PASS**（`overall_result/checker_result=PASS`，两个 exit=0，树指纹前后一致）；归档 `.evidence/drill-runner-env/remote/drill-37576884648/SUMMARY.txt`；**修复合入 main（`7587116`）后尚无远端重跑证据** → 当前 main 树 **NOT VERIFIED** |

## 5. 性能诊断（测量与定位，不宣称提升）

- **条件**：单主机、串行批次；013 消费者追赶分段测量 N=10000（pristine 书挡 + ON/OFF 同接缝配对）；
  `perf` 构建标签接缝在普通构建下为 no-op。
- **数字与来源**（示例）：尾窗覆盖 99.93%、`process` 占 tail 99.09–99.28%、语句链占 `tx_total` 98.8%
  —— 见 `docs/evidence/013-supplement/consumer-seg/README.md` 与
  `docs/evidence/013-supplement/consumer-seg/analysis/summary.md`（派生统计）；
  对照批次见 `docs/evidence/019-normal-query-seg/`；013 对照基准见 `docs/evidence/013/benchmark_report.md`。
- **局限（必须随数字引用）**：单机短窗口合成负载；样本量小（n≤4）与书挡自漂移使测点开销不可分解；
  **不构成生产容量结论，不构成 SLO**；容量/限流/告警/追赶阈值一律「待测/待裁决」。

## 6. 声明 → 证据对照（Claims-to-Evidence）

| 声明 | 证据 |
| --- | --- |
| PostgreSQL 唯一权威，Redis/Kafka 非权威 | `internal/events/catalog.go`（边界注释）、`internal/ratelimit/limiter.go`（包注释）、`docs/architecture.md` §4 |
| 幂等由 `UNIQUE(caller_id, idempotency_key)` 保证 | `migrations/000007_withdrawal_creation.sql`、`specs/007-withdrawal-creation/` |
| 重组：共同祖先 → 旧分叉失效 → `orphaned` → 重索引 | `migrations/000006_reorg_recovery.sql`、`internal/indexer/`、`specs/006-reorg-recovery/` |
| 确认深度 `max(0, tip − block + 1) ≥ N`，阈值版本化 | `specs/005-confirmation-tracking/`、`internal/indexer/confirm.go` |
| nonce 预留 + 持久绑定 + 对账 | `migrations/000008_nonce_manager.sql`、`internal/nonce/` |
| 私钥只在 Signer；`production` 无 provider 即拒绝启动 | `internal/signer/provider.go`、`internal/app/signerserve.go`、`specs/009-signer-service/` |
| 事务性 Outbox + 至少一次 + 消费幂等（不宣称恰好一次） | `migrations/000015_event_infrastructure.sql`、`internal/events/`、`specs/013-reliable-event-infrastructure/adr.md` |
| 限流失败策略：429 拒绝 / 仅新提现创建 503 / 其余继续原门禁 | `internal/ratelimit/policy.go` |
| 014 三路对账、检查点续跑、默认拒绝授权 | `migrations/000016_reconciliation_handling.sql`、`internal/reconciliation/doc.go` |
| 015 分级放行、非执行者批准（single/dual 按能力与副作用）、缺口阻塞、放行前 `POST /withdrawals` 503 | `internal/recovery/doc.go`、`specs/015-backup-recovery-safe-resumption/quickstart.md` §1–§2 |
| 崩溃边界测试（8 个硬杀子进程场景） | `internal/txlifecycle/crash_integration_test.go` |
| Drill NOT RUN 纪律（缺失/跳过即失败） | `Makefile`（`test-drill`）、`scripts/drillcoverage/check.go` |
| 版本/镜像/端口 | `go.mod`、`compose.yaml`、`compose.009.yaml` |

## 7. 未验证清单（NOT VERIFIED）

- 本文件整理期间**未执行任何层**；对当前 main（`6f9003a1`）除纯文档 CI 运行外无新验证。
- Mermaid：README 与 `docs/architecture.md` 的图已通过 `mermaid.parse`（jsdom 环境）语法解析
  （合计 4/4）；未做像素级渲染验证。
- 远端 Redis/E2E/Fault/Perf：有历史通过记录（§2/§3 所列运行，均为不同 head 的历史运行）；当前树未复跑。
- 远端 Drill：修复分支重跑已 PASS（[`37576884648`](https://github.com/xtianxx/TxHarbor/actions/runs/37576884648)，22/22 必验）；
  修复合入 main（`7587116`）后的远端重跑无留存证据。
- `docs/evidence/013/`、`docs/evidence/014/`、`docs/evidence/015/` 中标注为 `/tmp/opencode/` 的原始日志未入库；
  其持久载体为测试文件 + 提交 + 归档报告（各归档文件自述口径）。
