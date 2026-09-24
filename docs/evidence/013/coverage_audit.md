# 013 覆盖审计（T088）

- Feature: `013-reliable-event-infrastructure`
- 审计批次: B11（T088）；审计基准 HEAD: `65b2f07`（B1–B10 已合入；B11 改动为 CI/文档/证据/审计测试）
- 对照对象: `spec.md`（FR-01–FR-28、SC-01–SC-12、五态×七类矩阵、PD-1–PD-4）、`plan.md`（D1–D10/Blockers/Deferred）、`adr.md`（ADR-013-01/02、§3 基准、§5 Debezium 排除）、`tasks.md` 覆盖附录。
- 证据口径（三态分离，不得混同）:
  - **本地验收**：真实中间件 + 真实迁移（Anvil/PG/Redis/Kafka 容器）的批次运行；证据索引见 [quickstart_evidence_index.md](quickstart_evidence_index.md)。
  - **远程 CI**：`ci.yml`/`fault-perf.yml` 仅本地静态校验，GitHub runner 运行**待核验**（§5）。
  - **生产就绪**：未宣称；T000-P 保持 OPEN。
- 结论: 90/90 任务有成果与证据勾选；FR/SC/矩阵/ADR/PD 逐项对应如下；缺口/延期项在 §5 显式列出，**无静默扩缩范围**。

## 1. FR-01–FR-28 → 任务与证据

| FR | 任务（tasks.md） | 证据（持久载体 + 批次记录） | 状态 |
|---|---|---|---|
| FR-01 PG 唯一权威 | T009, T014, T016, T020, T059, T068, T085 | `internal/db/event_infrastructure_migration_integration_test.go`；`internal/events/boundary_test.go`；`internal/faultdrill/matrix_contract_test.go`（门禁不读 Redis/Kafka）；T085 审计 | 已覆盖（本地） |
| FR-02 Redis/Kafka 用途限定 | T003, T020, T059–T064, T085 | 同上 + `internal/cache/*_test.go`、`internal/ratelimit/*_test.go`；T085 import 边界 | 已覆盖（本地） |
| FR-03 不引入其他基础设施 | T002, T020, T085 | `go.mod`/`compose.yaml`/CI 扫描 0 处 Debezium/CDC/Kubernetes（T085）；ADR §5 | 已覆盖（本地） |
| FR-04 故障矩阵 | T017–T025, T080, T089, T090 | `internal/faultdrill/drill_test.go`、`state_*.go`、`matrix_invariants_test.go`；`/tmp/opencode/b9_evidence_t025/{matrix.json,invariants.json}`、`b9_evidence_drill/`、`b10_fault_final.log`（Q8） | 已覆盖（本地） |
| FR-05 门禁继承/重复投递不触发新效果 | T030, T041, T049, T054, T080, T089 | `replay_boundary_integration_test.go`（0 新意图/nonce/签名/广播）、`outbox_execution_atomicity_integration_test.go`、`matrix_invariants_test.go`（Q1/Q4/Q8） | 已覆盖（本地） |
| FR-06 故障不变量（七项 0） | T025, T080 | `b9_evidence_t025/invariants.json`、`matrix_invariants_test.go`；`b10_fault_final.log`（Q8） | 已覆盖（本地） |
| FR-07 同事务 Outbox + 目录 | T008, T011, T026–T032, T038–T040 | `append_integration_test.go`、`outbox_atomicity_integration_test.go`、`outbox_intake_atomicity_integration_test.go`、`catalog_conformance_integration_test.go`（Q1） | 已覆盖（本地） |
| FR-08 至少一次发布/崩溃恢复/有界尝试 | T033–T037 | `publisher_integration_test.go`（崩溃点矩阵/租约/错 owner）、`publisher_kafka_integration_test.go`；`kafka_layer.log`（Q2） | 已覆盖（本地） |
| FR-09 事件身份 UNIQUE/冲突告警 | T010, T011, T014, T015, T016 | `append_integration_test.go`（23505 分类/冲突计数）、`events_contract_test.go`；`b2-events-integration.log`（Q1） | 已覆盖（本地） |
| FR-10 版本顺序与链身份 | T010, T041, T046, T051 | `consumer_integration_test.go`（乱序/缺口/旧不覆盖新）、`consumer_contract_test.go`（Q3） | 已覆盖（本地） |
| FR-11 重组修订 | T028, T052–T058 | `revision_events_integration_test.go`、`execution_revision_events_integration_test.go`、`revision_consumer_integration_test.go`、`revision_contract_test.go`；`b6-*`（Q11） | 已覆盖（本地） |
| FR-12 schema 版本兼容/fail-closed | T014, T042, T051, T058 | `events_contract_test.go`、`consumer_contract_test.go`、`revision_contract_test.go`、`quarantine_integration_test.go`（未知版本隔离）（Q3/Q4/Q11） | 已覆盖（本地） |
| FR-13 消费者持久幂等 | T041, T046, T048, T050 | `consumer_integration_test.go`、`consumer_kafka_integration_test.go`（inbox UNIQUE、效果=1）（Q3） | 已覆盖（本地） |
| FR-14 有界重试/隔离/重放 PD-4 | T042, T045, T048, T049 | `quarantine_integration_test.go`、`replay_boundary_integration_test.go`（审计行/operation_id 去重/重放幂等）（Q4） | 已覆盖（本地） |
| FR-15 进度持久/续传/lag | T041, T044, T047, T050 | `consumer_progress_integration_test.go`（offset 丢失后 100% 续传）、`consumer_kafka_integration_test.go`（Q3） | 已覆盖（本地） |
| FR-16 入账边界声明 | T043, T081 | `internal/events/refconsumer.go`；`b10_t081/{boundary.md,ledger_counts.json}`；`b9_evidence_*/boundary.md`（Q8） | 已覆盖（本地） |
| FR-17 缓存查询行为 | T059, T060, T063, T065, T068 | `cache_integration_test.go`、`invalidator_integration_test.go`、serve 降级集成（Q5） | 已覆盖（本地） |
| FR-18 PD-1 限流失效 | T062, T063, T066, T067 | `ratelimit_integration_test.go`、`ratelimit_failure_integration_test.go`（双验收线）（Q6） | 已覆盖（本地） |
| FR-19 RPC 降级不破坏契约 | T064, T067 | `internal/ratelimit/rpcbudget.go` 单测 + `ratelimit_failure_integration_test.go`（分类/完整性不跳过）（Q6） | 已覆盖（本地） |
| FR-20 PD-2 容量保护 | T069–T074, T089, T090 | `capacity_integration_test.go`、`capacity_wiring_integration_test.go`、`capacitypause` 单测 + B9 演练（Q7/Q8） | 已覆盖（本地） |
| FR-21 发布器恢复排空 | T033, T034, T074 | `publisher_catchup_integration_test.go`、`publisher_kafka_integration_test.go`（Q2/Q9） | 已覆盖（本地） |
| FR-22 消费者追赶/再故障 | T073, T080 | `catchup_test.go`、`state_catchup_refailure_test.go`、`drill_test.go`（Q9） | 已覆盖（本地） |
| FR-23 追赶期缓存/限流重建 | T060, T063, T068 | `invalidator_integration_test.go`（epoch/惰性重建）、`ratelimit_integration_test.go`（梯度恢复）（Q5/Q6） | 已覆盖（本地） |
| FR-24 观测 | T003, T075, T085 | `internal/metrics/events_test.go`、`alerts_test.go`；`b10_alerts_final.log`；T085 脱敏/边界审计 | 已覆盖（本地） |
| FR-25 对照基准与 ADR | T076, T077, T087 | `docs/evidence/013/benchmark_report.md`（绑定 `89ef787`）；ADR §3/§6 实施记录（Q10） | 已覆盖（本地；未测项标注） |
| FR-26 故障演练验收 | T019–T025, T080, T089, T090 | V-DRILL 证据包（Q8）；本地确定性环境，无公网依赖 | 已覆盖（本地） |
| FR-27 范围留白纪律 | T001, T002, T085, T088 | 配置初始值标注「测量后校准」；T085 审计；本文件 §5 缺口显式化 | 已覆盖 |
| FR-28 测试分层与 CI 成本 | T005, T006, T082–T085 | `Makefile` 分层 target + `require_tagged_tests`；`ci.yml`/`fault-perf.yml`；`layering_audit_test.go`；远程运行待核验（§5） | 已覆盖（本地）；远程待核验 |

## 2. SC-01–SC-12 → 证据与断言

| SC | 证据/断言 | 任务 | 状态 |
|---|---|---|---|
| SC-01 矩阵一致率 100%、门禁绕过 0 | `b9_evidence_t025/matrix.json` + `invariants.json`；`drill_test.go` | T021–T025, T080, T089, T090 | 本地通过 |
| SC-02 0 重复意图/付款/孤儿入账/权威丢失 | `matrix_invariants_test.go`、`replay_boundary_integration_test.go` | T025, T049, T080 | 本地通过 |
| SC-03 状态↔事件 100%/0、重复投递财务重复 0 | `outbox_*_atomicity_integration_test.go`、`consumer_integration_test.go`（参考消费者） | T014, T015, T038–T040, T046 | 本地通过 |
| SC-04 有效应用 1、旧不覆盖新、缺口不静默 | `consumer_integration_test.go`、`consumer_kafka_integration_test.go` | T046, T050 | 本地通过 |
| SC-05 毒事件隔离 100%、无界重试 0、静默丢弃 0 | `quarantine_integration_test.go`、`replay_boundary_integration_test.go`、`consumer_contract_test.go` | T042, T048, T049, T051 | 本地通过 |
| SC-06 进度 100% 恢复、重放重复效果 0 | `consumer_progress_integration_test.go`、`consumer_kafka_integration_test.go` | T047, T050 | 本地通过 |
| SC-07 修订幂等、Orphaned 误用 0、新付款 0 | `revision_*_test.go`（T055–T058） | T055–T058 | 本地通过 |
| SC-08 陈旧权威 0、无限制放行 0、恢复后陈旧 0 | `cache_integration_test.go`、`invalidator_integration_test.go`、`ratelimit_integration_test.go`、`ratelimit_failure_integration_test.go` | T065–T068 | 本地通过 |
| SC-09 停机 0 丢失、可观测 100%、PD-2、排空有界 | `capacity_integration_test.go`、`capacity_wiring_integration_test.go`、`publisher_catchup_integration_test.go`、`state_kafka_test.go` | T069–T074, T089, T090 | 本地通过 |
| SC-10 再故障 0 丢失/0 重复、进度可续 | `catchup_test.go`、`state_catchup_refailure_test.go` | T073, T080 | 本地通过 |
| SC-11 对照报告 100%、未测不宣称 | `benchmark_report.md`（含 §9 未测清单） | T076, T077 | 本地通过 |
| SC-12 证据覆盖矩阵、边界声明、外部保证 0 | `b9_evidence_*/boundary.md`、`b10_t081/boundary.md`、`matrix_invariants_test.go`；`coverage_audit.md`（本文件） | T080, T081, T086 | 本地通过 |

## 3. 五态 × 七类矩阵 → 任务与验证

| 操作 | 正常 | 仅 Redis 故障 | 仅 Kafka 故障 | 双故障 | 恢复追赶 |
|---|---|---|---|---|---|
| 充值处理 | T035/T038（Q1） | T021（Q5/Q6 缓存旁路） | T022（Q2 积压） | T023/T080（Q8） | T024/T073（Q9） |
| 确认与重组恢复 | T027/T028/T055（Q1/Q11） | T021 | T022（修订积压） | T023/T080 | T024/T057（Q11） |
| 提款创建（接收） | T029/T070（Q1/Q7） | T021/T067（PD-1，Q6） | T022/T072（PD-2，Q7） | T023/T080 | T024（Q9） |
| 已有提款执行 | T030/T056（Q1/Q11） | T021/T067 | T022 | T023/T080 | T024 |
| 查询 | T018/T063（Q5） | T021/T065（Q5） | T022 | T023/T080 | T024 |
| 事件订阅 | T033–T037（Q2） | T021 | T022/T034/T074（Q2/Q9） | T023/T080 | T024/T073/T074（Q9） |
| 非关键功能 | T018（降级开关测试） | T021 | T022 | T023/T080 | T024 |

**安全前提行**：0 门禁绕过（T020/T025/T080）；不跳过链身份/完整性校验（T064/T067）；链上已发生充值不拒绝、无法持久化则暂停补扫（T071/T072/T090）；在途提款不新建意图（T049/T071/T090）；不静默丢弃/不覆盖未发布（T069/T072）。以上均由 Q8 证据包与对应测试断言覆盖（本地）。

## 4. ADR-013-01/02 与 PD-1–PD-4 → 任务与实施记录

| 决策 | 任务 | 实施/验证记录 |
|---|---|---|
| ADR-013-01 Redis 非权威缓存 + 分布式限流 | T002, T059–T068, T077, T087 | adr.md §6 实施记录；提交 `96c2c28`；Q5/Q6 本地通过；T085 边界审计 |
| ADR-013-02 Kafka + 事务性 Outbox | T008, T011, T033–T050, T076, T077 | adr.md §6 实施记录；提交 `23561da`…`4da2ca1`；Q1–Q4/Q8/Q9 本地通过 |
| ADR §3 同负载对照基准 | T076, T077 | `benchmark_report.md`；同负载同故障 A/B；置信限制与未测清单 |
| ADR §5 Debezium 明确不引入 | T002, T085 | `go.mod`/`compose.yaml`/CI 扫描 0 处；T085 审计 |
| PD-1 限流失效处置 | T062, T063, T066, T067, T021, T023 | 「拒绝新创建」与「存量继续」分别验收（Q6/Q8） |
| PD-2 容量保护 | T069–T074, T089, T090, T022, T023 | 停新保在途、补扫、在途完成、再故障 0 丢失/0 重复（Q7/Q8/Q9） |
| PD-3 Kafka 价值证明与范围保持 | T076, T077, T087 | 基准已报告；Kafka 范围不变、不自动调整；调整须另行提交用户决定 |
| PD-4 人工重放边界 | T045, T048, T049, T050 | 审计/幂等/不得重付；反例断言 0 新意图/nonce/签名/广播（Q4） |

## 5. 缺口 / 延期项（显式列出，归属与理由）

| 项 | 状态 | 归属/理由 |
|---|---|---|
| GitHub runner 上的 CI / fault-perf 实际运行 | **待核验**（未推送、未触发） | B11 范围明确禁止推送/PR；workflow 已完成本地静态校验（YAML/`bash -n`/触发矩阵断言），远程证据缺失，不得标通过 |
| 目标 runner 各层耗时基线与预算 | 本地基线已实测；远程列待核验 | `ci_budget.md`；未推送无法在 GitHub runner 测量，未编造数值 |
| 生产阈值（soft/hard/reserve/retention、限流速率、退避/超时、追赶窗口、告警阈值） | 待测/待裁决 | plan Deferred；benchmark_report.md §9；不得提前写死 |
| Kafka 价值结论与范围调整 | 保持范围内；调整须另行提交用户决定 | PD-3；benchmark 报告不自动删减/扩大 |
| 长稳（小时级）、多主机/多实例基准、生产负载回放 | 未测 | benchmark_report.md §9；不构成生产结论 |
| 013 分支合并/部署 | 未合并/未部署 | 本批禁止推送/PR/合并/部署；T000-P 保持 OPEN |
| `internal/cache` 单飞测试（`TestClientFallbackSingleflightCollapsesSameKey`，Unit 载体/fake store，时序敏感）在 B11 测量中出现偶发失败 | **风险登记（未闭合）**：隔离探针 12 次 1 红；全量 `make test-race` 6 次 3 红；全量 `make test` 3 次红；Integration-Redis 首次 1 红；失败均为 loader 调用数 > 1（无数据竞争、无资金断言失败），同树复跑可绿；B10 记录全绿 | 既有 B7（T059）交付物；B11 授权范围不允许修改 cache 代码/测试；机制分析、加固建议与证据见 `ci_budget.md` §4；在此之前该层偶发红不得读作产品回归 |
| `event_system_state` 是否可裁剪为配置 | 已确认保留 | plan Deferred 项由 T031 实现批次确认 |
| A-13（011 全链 E2E） | 已 CLOSED（沿用上游记录，不重开） | plan Deferred |

## 6. 审计结论

- FR-01–FR-28、SC-01–SC-12、五态×七类矩阵、ADR-013-01/02、PD-1–PD-4 均有对应任务与本地证据；未发现设计矛盾或裁决边界不可满足。
- 本审计未扩缩任何范围：缺口全部显式列出（§5），无静默跳过、无「以提交宣告全部验收」。
- **本地验收通过 ≠ 远程 CI 通过 ≠ 生产就绪**；013 分支未合并/未部署，T000-P 保持 OPEN。
