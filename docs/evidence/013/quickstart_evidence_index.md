# 013 Quickstart 证据索引（Q0–Q11）

- Feature: `013-reliable-event-infrastructure`
- 生成批次: B11（T086）
- 基准 HEAD: `65b2f07`（B1–B10 已合入；B11 新增 CI/文档/审计）
- 状态口径（三者分离，不得混同）:
  - **本地验收**：本文索引的「通过」均为本地确定性环境（Anvil + PG + Redis + Kafka 容器）中真实中间件与真实迁移的运行结果（批次 B1–B10 记录 + B11 复跑），非 mock/double 证据。
  - **远程 CI**：`.github/workflows/ci.yml`（T082）与 `fault-perf.yml`（T083）尚未在 GitHub runner 上运行过，远程运行证据**待核验**（见 `coverage_audit.md` §5）。
  - **生产就绪**：未宣称；T000-P 保持 OPEN。
- 原始证据目录位于 `/tmp/opencode/`（机器本地、**未入库**）；入库的持久载体是测试文件、提交哈希与 `docs/evidence/013/` 下的报告。原始日志若被清理，以测试文件 + 提交 + 批次退出记录为准。
- 全部「通过」不得解读为阈值达标：容量/限流/告警/追赶阈值仍为「待测/待裁决」（benchmark_report.md §9）。

## 索引总表

| Q | 场景 | 批次 | 层（FR-28） | 结果（本地） | 主要证据 |
|---|---|---|---|---|---|
| Q0 | Cutover 基线与快照导出 | B2/B3 | Integration-PG | 通过 | `internal/db/event_infrastructure_migration_integration_test.go`、`internal/app/eventsadmin_b3_integration_test.go`；`/tmp/opencode/b2-t009-migration.log`、`/tmp/opencode/b3-t031-evidence.log` |
| Q1 | 同事务原子性探针 | B2/B3 | Integration-PG | 通过 | `internal/events/append_integration_test.go`、`internal/indexer/outbox_atomicity_integration_test.go`、`internal/withdrawal/outbox_intake_atomicity_integration_test.go`、`internal/execution/outbox_execution_atomicity_integration_test.go`；`/tmp/opencode/b2-events-integration.log`、`/tmp/opencode/b3-*` |
| Q2 | 发布器崩溃/重复/多实例 | B4/B8 | Integration-Kafka + PG | 通过 | `internal/events/publisher_integration_test.go`、`publisher_kafka_integration_test.go`、`publisher_catchup_integration_test.go`；`/tmp/opencode/b5-kafka.log`、`/tmp/opencode/kafka_layer.log`、`/tmp/opencode/kafka_integration_out.txt` |
| Q3 | 消费幂等与版本守卫 | B5 | Integration-PG + Kafka | 通过 | `internal/events/consumer_integration_test.go`、`consumer_progress_integration_test.go`；`/tmp/opencode/b5-integration-p1.log`、`/tmp/opencode/b5-kafka.log` |
| Q4 | 重试/隔离/人工重放（PD-4） | B5 | Integration-PG | 通过 | `internal/events/quarantine_integration_test.go`、`replay_boundary_integration_test.go`；`/tmp/opencode/b5-integration-p1.log` |
| Q5 | 缓存陈旧与击穿保护 | B7 | Integration-Redis | 通过 | `internal/cache/cache_integration_test.go`、`invalidator_integration_test.go`；`/tmp/opencode/redis_final.log`、`redis_layer.log` |
| Q6 | 限流失效处置（PD-1） | B7/B5 | Integration-Redis + E2E | 通过 | `internal/ratelimit/ratelimit_integration_test.go`、`internal/app/ratelimit_failure_integration_test.go`；`/tmp/opencode/redis_final.log`、`/tmp/opencode/b10_e2e_final.log` |
| Q7 | 容量保护「停新保在途」 | B8 | Integration-PG | 通过 | `internal/events/capacity_integration_test.go`、`internal/app/capacity_wiring_integration_test.go`；`/tmp/opencode/pg_integration_out.txt`、`/tmp/opencode/b10_pg_suite_final.log` |
| Q8 | 五态故障矩阵演练（V-DRILL） | B9 | Fault（独立） | 通过 | `internal/faultdrill/drill_test.go`、`state_dual_test.go`、`matrix_invariants_test.go`；`/tmp/opencode/b9_evidence_t025/`、`/tmp/opencode/b9_evidence_drill/`、`/tmp/opencode/b9_fault_full/`、`/tmp/opencode/b10_fault_final.log` |
| Q9 | 恢复追赶与再故障 | B8/B9 | Fault（独立） | 通过 | `internal/faultdrill/catchup_test.go`、`state_catchup_refailure_test.go`、`internal/events/publisher_catchup_integration_test.go`；`/tmp/opencode/catchup_out3.txt`、`/tmp/opencode/b10_fault_final.log` |
| Q10 | PG-only vs 全栈对照基准 | B10 | Performance（独立） | 通过（报告 100% 产出；未测项标注） | `docs/evidence/013/benchmark_report.md`；`/tmp/opencode/b10_perf_final/` |
| Q11 | 重组修订与 block_hash 复活 | B6 | Integration-PG + Contract | 通过 | `internal/indexer/revision_events_integration_test.go`、`internal/execution/execution_revision_events_integration_test.go`、`internal/events/revision_consumer_integration_test.go`、`revision_contract_test.go`；`/tmp/opencode/b6-revision-matrix.log`、`b6-contract-matrix.log`、`b6-integration-final.log` |

## 逐场景记录（命令、结果、证据）

复现命令使用各层 Makefile 入口 + `-run` 聚焦；批次门禁使用的是整层运行（`make test-integration` / `-redis` / `-kafka` / `-contract` / `-e2e` / `-fault` / `-perf`），结果见对应日志。

### Q0 — Cutover 基线与快照导出（B2 T009 / B3 T031）
- 命令（复现）:
  - `go test -tags integration -count=1 -run TestEventInfrastructureMigrationUpDownUp ./internal/db`
  - `go test -tags integration -count=1 -run TestEventsAdminCutoverPath ./internal/app`
- 结果: 通过（up/down/up 干净、约束负向探针命中 23505/23514 且按 ConstraintName 分类；导出前后业务表计数不变、无 cutover 前补造事件）。
- 证据: `/tmp/opencode/b2-t009-migration.log`、`/tmp/opencode/b3-t031-evidence.log`；提交 `23561da`（B1）/`e126330`（B2）/`53ccbc6`（B3）。
- 备注: 「仅停发射而业务继续」为违规、不作回退方案，记录于 B3 回退步骤（tasks T031）。

### Q1 — 同事务原子性探针（B2 T015 / B3 T038–T040）
- 命令（复现）:
  - `go test -tags integration -count=1 -run 'TestAppend|TestOutbox' ./internal/events ./internal/indexer ./internal/withdrawal ./internal/execution`
- 结果: 通过（提交必有事件行且同对象版本连续；回滚两者皆无；重复同身份 no-op；异内容拒绝 + 冲突计数；`withdrawal.execution.state_changed` 未知结果尝试 attempt 引用 100% 存在）。
- 证据: `/tmp/opencode/b2-events-integration.log`、`/tmp/opencode/b3-indexer-full.log`、`/tmp/opencode/b3-withdrawal-full-rerun.log`、`/tmp/opencode/b3-t040-attempt-evidence.log`、`/tmp/opencode/b10_fund_safety_regression.log`（B10 复跑含 T039 幂等/重放基线）。
- 备注: B10 确认循环修复（纯前向推进、捕获块 canonical→errStaleState、零写入）由 `confirmation_tipadvance_integration_test.go` + TipMismatch 拆分覆盖，B10 已绿（`/tmp/opencode/b10_tipadvance_race.log`）；B11 未改 indexer。

### Q2 — 发布器崩溃/重复/多实例（B4 T036/T037 / B8 T074）
- 命令（复现）:
  - `go test -tags integration -count=1 -run TestPublisher ./internal/events`
  - `go test -tags integration_kafka -count=1 -run 'TestPublisherKafka|TestPublisherCatchup' ./internal/events ./internal/app`
- 结果: 通过（同刻单 owner、租约重领、错 owner 0 行、崩溃点矩阵 0 丢失、blocked 可见可审计、未提交行永不发布；真实 broker 投递确认、重复可吸收、排空有界、再故障 0 丢失）。
- 证据: `/tmp/opencode/b5-kafka.log`、`/tmp/opencode/kafka_layer.log`、`/tmp/opencode/kafka_integration_out.txt`；提交 `cba7a9b`（B4）/`55a742b`（B8）。

### Q3 — 消费幂等与版本守卫（B5 T046/T047/T050）
- 命令（复现）:
  - `go test -tags integration -count=1 -run 'TestConsumer' ./internal/events`
  - `go test -tags integration_kafka -count=1 -run 'TestConsumer' ./internal/events`
- 结果: 通过（重复投递/重启/rebalance 有效应用次数 = 1；旧版本 0 覆盖；缺口有界等待后隔离且分区继续；offset 丢失后 100% 从 PG 进度续传；0 静默跳过）。
- 证据: `/tmp/opencode/b5-integration-p1.log`、`/tmp/opencode/b5-kafka.log`；提交 `eb4e06b`（B5）。

### Q4 — 重试/隔离/人工重放（B5 T048/T049，PD-4）
- 命令（复现）:
  - `go test -tags integration -count=1 -run 'TestConsumerRetry|TestConsumerQuarantine|TestReplayBoundary|TestUnblock' ./internal/events`
- 结果: 通过（可重试有界退避、超限/不可重试/未知版本/身份不匹配持久隔离且可审计、重放幂等、`operation_id` 去重；重放/解阻后 0 新提款意图、0 新 nonce、0 新签名、0 新广播；自动重试与人工重放指标分列）。
- 证据: `/tmp/opencode/b5-integration-p1.log`；提交 `eb4e06b`（B5）。

### Q5 — 缓存陈旧与击穿保护（B7 T065/T068）
- 命令（复现）: `go test -tags integration_redis -count=1 -run 'TestCache|TestInvalidator' ./internal/cache`
- 结果: 通过（权威状态变化后 0 陈旧财务权威；关/清 Redis 直读 PG 且回源并发有界；恢复后 epoch 轮换 0 已失效值；新鲜度标注正确）。
- 证据: `/tmp/opencode/redis_final.log`、`/tmp/opencode/redis_layer.log`；提交 `96c2c28`（B7）。
- 备注（B11 观察）: B11 测量中 `internal/cache` 的 `TestClientFallbackSingleflightCollapsesSameKey`（Unit 载体、fake store、时序敏感）出现偶发失败：隔离探针 12 次 1 红；全量 `make test-race` 6 次 3 红；全量 `make test` 3 次红；失败均为 loader 调用数 > 1（无数据竞争、无资金断言失败），同树复跑可绿；B10 记录全绿。判定为既有测试的时序断言窗口问题（非产品回归、非 B11 改动），机制分析与加固建议见 `ci_budget.md` §4；B11 未修改 cache 代码/测试。

### Q6 — 限流失效处置（B7 T066 / B5 T067，PD-1）
- 命令（复现）:
  - `go test -tags integration_redis -count=1 -run TestRatelimit ./internal/ratelimit`
  - `go test -tags e2e -count=1 -run TestRatelimitFailureHTTPPolicy ./internal/app`
- 结果: 通过（停 Redis → 新提款创建 100% 拒绝且 429/503 + Retry-After、0 无限制放行；存量充值/确认/已接受提款继续且 0 门禁绕过；恢复梯度放开；限流不参与授权判定）。
- 证据: `/tmp/opencode/redis_final.log`、`/tmp/opencode/b10_e2e_final.log`；提交 `96c2c28`（B7）/`eb4e06b`（B5 E2E）。

### Q7 — 容量保护（B8 T072/T089/T090）
- 命令（复现）:
  - `go test -tags integration -count=1 -run 'TestCapacityGuard|TestCapacity' ./internal/events ./internal/app ./internal/indexer`
- 结果: 通过（`pending ≥ soft` → 可控新写入 100% 拒绝且可重试、链上事实不拒绝/按可靠进度暂停、在途完成且事件齐备、0 静默丢弃/0 覆盖、恢复排空；`capacity_refusals_total` 可观测）。
- 证据: `/tmp/opencode/pg_integration_out.txt`、`/tmp/opencode/b10_pg_suite_final.log`、`/tmp/opencode/b10_fund_safety_regression.log`；提交 `55a742b`（B8）/`4da2ca1`（B9 接线）。

### Q8 — 五态矩阵演练（B9 T080/T025，V-DRILL）
- 命令（复现）: `make test-fault`（聚焦：`go test -tags fault -count=1 -run 'TestDrillFiveStateMatrix|TestStateDualFault' ./internal/faultdrill`）
- 结果: 通过（正常→仅 Redis→仅 Kafka→双故障→恢复追赶逐态核对；矩阵一致率 100%、门禁绕过 0、五项 0 不变量：0 重复提款意图/0 重复链上付款/0 孤儿永久入账/0 权威状态丢失/0 静默事件丢失；证据含模拟消费者入账幂等与 FR-16 边界声明）。
- 证据: `/tmp/opencode/b9_evidence_t025/`（matrix.json、invariants.json、metrics_*.txt、timeline.jsonl、boundary.md）、`/tmp/opencode/b9_evidence_drill/`、`/tmp/opencode/b9_fault_full/`、`/tmp/opencode/b10_fault_final.log`（B10 终次 `make test-fault` 全绿，`internal/faultdrill 751.6s`）；提交 `4da2ca1`（B9）。
- 备注: 原始证据目录未入库（`/tmp/opencode/...`）；持久载体为 fault-tagged 测试与提交。

### Q9 — 恢复追赶与再故障（B8 T073/T074 / B9 T024）
- 命令（复现）: `make test-fault`（聚焦：`go test -tags fault -count=1 -run 'TestCatchupRecoveryAndRefailure' ./internal/faultdrill`）
- 结果: 通过（恢复排空与消费者追赶；追赶中再停 Kafka/Redis → 安全重暂停、0 丢失、0 重复财务效果、进度可续；追赶时间可观测、数值待测）。
- 证据: `/tmp/opencode/catchup_out3.txt`、`/tmp/opencode/b10_fault_final.log`；提交 `55a742b`（B8）/`4da2ca1`（B9）。

### Q10 — PG-only vs 全栈对照基准（B10 T077，V-BENCH）
- 命令（复现）: `TXHARBOR_PERF_REPEATS=2 make test-perf`
- 结果: 报告 100% 产出（路径 A/B × 五态 × 指标、置信限制、未测清单）；未测数值 0 次表述为达标。
- 证据: `docs/evidence/013/benchmark_report.md`（绑定 commit `89ef787`，标签修正提交 `65b2f07`）；原始证据 `/tmp/opencode/b10_perf_final/`；提交 `89ef787`（B10）。
- 限制（必须随引用携带）: 本地单主机、短窗口、合成负载，**非生产结论**；正常态查询 p95 A=47.5ms / B=72.7ms、Redis 故障回源约 1005ms 仅为本机本负载观测；Kafka 范围按 PD-3 不变、不自动调整。

### Q11 — 重组修订与 block_hash 复活（B6 T055–T058）
- 命令（复现）:
  - `go test -tags integration -count=1 -run 'TestT055|TestT056|TestT057' ./internal/indexer ./internal/execution ./internal/events`
  - `go test -tags contract -count=1 -run 'TestRevision|TestEventsContract|TestConsumerContract' ./internal/events`
- 结果: 通过（修订携带旧身份/新状态或 Orphaned/原因/恢复版本、`revises_event_id` 指向旧事件、复活与新观察不混淆、修订幂等、0 新发送、0 次把 Orphaned 当有效 canonical；8/8 目录矩阵收口）。
- 证据: `/tmp/opencode/b6-revision-matrix.log`、`/tmp/opencode/b6-contract-matrix.log`、`/tmp/opencode/b6-integration-final.log`、`/tmp/opencode/b6-kafka-final.log`；提交 `cff0f8d`（B6）。

## B11 复跑与新增检查（本批次）

| 项 | 命令 | 结果 | 证据 |
|---|---|---|---|
| Unit（含 T085 审计测试） | `make test` | 审计测试与全部非 cache 包通过；cache 单飞测试偶发（全量复跑 3 次红，失败样本 `loader calls = 13`）；同树复跑绿 | `/tmp/opencode/b11_ci_budget/unit-final.log`、`final-test.log`、`unit_probe_*.log`；`ci_budget.md` §4 |
| Race | `make test-race` | 6 次 3 红（同一 cache 单飞测试，`loader calls = 2`）；其余包通过 | `/tmp/opencode/b11_ci_budget/race-final.log`、`race_probe_*.log`、`race-rerun2.log`；`ci_budget.md` §4 |
| Contract | `make test-contract` | 通过（7s） | `/tmp/opencode/b11_ci_budget/contract-final.log` |
| Lint / Build | `make lint` / `make build` | 通过（2s / 4s） | `/tmp/opencode/b11_ci_budget/final-lint.log`、`final-build.log` |
| Integration-PG / Redis / Kafka / E2E（B11 基线测量） | `make test-integration` / `-redis` / `-kafka` / `-e2e` | 通过（311s / 50s 复跑 / 184s / 67s 复跑） | `/tmp/opencode/b11_ci_budget/integration-pg.log`、`integration-redis-rerun.log`、`integration-kafka.log`、`e2e-rerun.log` |
| 分层审计（T085） | `go test -count=1 -run TestT085 ./internal/app` | 通过（B11 新增） | `internal/app/layering_audit_test.go` |
| CI workflow 静态校验 | `python3` YAML + `bash -n` + 触发分类用例（9 组：docs-only / events / migration / cache-ratelimit / 上游集成点 / publisher / app / faultdrill-only / workflow） | 通过（远程运行待核验） | B11 报告；`ci_budget.md` §5 |

## 未执行/待核验（显式标注，不得伪装通过）

| 项 | 状态 |
|---|---|
| GitHub runner 上的 CI/fault-perf 实际运行 | **待核验**（本批仅本地静态校验与本地分层运行；未推送/未触发远程 CI） |
| 各层在目标 runner 的耗时基线 | 待测（本地基线已记录，见 `ci_budget.md`；远程列待核验） |
| 生产阈值（容量/限流/告警/追赶窗口）校准 | 待测/待裁决（benchmark_report.md §9） |
| 长稳（小时级）、多主机/多实例基准、生产负载回放 | 未测（benchmark_report.md §9） |
