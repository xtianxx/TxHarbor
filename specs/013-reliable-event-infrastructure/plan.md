# Implementation Plan: 013 Reliable Event Infrastructure（可靠事件基础设施）

**Branch**: `013-reliable-event-infrastructure` | **Date**: 2026-09-24 | **Spec**: [spec.md](spec.md)（clarify 2026-09-23 + 2026-09-24 收口；PD-1–PD-4 均已裁决；`[NEEDS CLARIFICATION]` = 0）

**Input**: Feature specification `specs/013-reliable-event-infrastructure/spec.md`（FR-01–FR-28、SC-01–SC-12、故障服务矩阵、PD-1–PD-4、`### Session 2026-09-23/2026-09-24`）；Charter Constitution 1.1.0（`.specify/memory/constitution.md`）；只读上游契约 002–011/PB（索引见 spec Background；实现证据沿用各自记录，不重新核验）；已交付代码与迁移 `000001`–`000014`；prior art `specs/011-withdrawal-executor/{plan,research,data-model,contracts,quickstart}.md`、`specs/012-007-authorization-carrier/plan.md`。

**Note**: 本 plan 只做 Phase 0/1 设计：不产出 tasks.md，不改业务代码、迁移、CI 工作流、compose.yaml；`spec.md` 第 9 行 Input 历史引文保留不动。ADR 与验收设计模板无对应章节，按任务要求独立成文：决策记录见 [adr.md](adr.md)（FR-25/PD-3），观测/验收/测试分层见 [verification.md](verification.md)（FR-24/FR-28）。阈值类数值一律给测量方法与配置边界；确需初始值的仅限技术参数，明确标注「初始值，测量后校准」，不冒充已批准业务阈值。

## Summary

为 TxHarbor 引入第 013 号能力：Redis（缓存 + 分布式限流）与 Kafka（异步事件投递），并以**事务性 Outbox** 替代「先写库再发布」双写。核心设计：

1. **同事务 Outbox**：所有目录内的业务状态转换与其事件记录在同一 PostgreSQL 事务提交（`Append` 辅助接口 + 集成点清单 + 对账审计兜底），事件身份分两类自然键并加部分 UNIQUE：EVM 日志 `(chain_id, block_hash, tx_hash, log_index)`、业务对象 `(aggregate_type, aggregate_id, aggregate_version)`；同身份异内容拒绝并告警。
2. **至少一次发布 + 幂等处理**：多实例发布器用 `FOR UPDATE SKIP LOCKED` + 租约领取，`acks=all` + 幂等 producer 投递并持久化发布标记；消费者用持久 `consumer_inbox` UNIQUE 去重、`consumer_versions` 版本守卫、有界重试、持久隔离区与可审计人工重放；进度存 PostgreSQL + Kafka offset 双载体。准确表述：**至少一次投递 + 幂等处理，不宣称跨系统恰好一次**。
3. **故障矩阵落地**：五态（正常/仅 Redis 故障/仅 Kafka 故障/双故障/恢复追赶）× 七类操作，按 PD-1（限流失效拒绝新提款创建 + 存量继续）、PD-2（停新保在途、预留恢复容量、链上充值不拒绝）实现；PG 权威与上游门禁不依赖 Redis/Kafka。
4. **重组修订事件**：旧身份 + 新 canonical 状态/Orphaned 处置 + 原因 + 恢复版本；幂等、非付款指令、不触发新意图/发送。
5. **容量保护**：Outbox 待发量与最老等待可观测；软边界先拒绝可控制的新资金写入、硬边界前预留容量，保证「已接纳的在途可完成」与「Outbox 接近满」不矛盾（停新保在途 + 原子提交不变量）。
6. **价值证明与验收**：ADR 记录 Redis/Kafka 引入理由、PG-only 替代、复杂度/故障/运维成本与同负载对照基准设计；验收按 FR-28 分层（Unit / 分组件 Integration / Contract / 核心 E2E / 独立 Fault Injection / 独立 Performance），普通 PR 不跑完整矩阵。

本计划新增运行时进程：`event-publisher`（发布器）、`events-admin`（操作员：隔离区/重放/解阻）、`event-consumer`（参考消费者，用于验收证据，明确非生产账本）；`serve` 扩展缓存与限流接线；新增一个纯增量迁移 `000015`；compose 增加 redis/kafka（实现阶段，本文只定义形状）。T000-P 保持 OPEN，不宣称生产就绪。

## Technical Context

**Language/Version**: Go 1.26.5（`go.mod`，仓库工具链）。金额/版本/计数一律整数；本 feature 不新增金融数值路径。

**Primary Dependencies**: 既有 pgx v5.11.0、goose v3.28.0、go-ethereum v1.17.5、prometheus/client_golang、testcontainers-go（含 postgres 模块）。**新增运行时依赖（提案，实现阶段验证后固定）**：Kafka 客户端 `github.com/twmb/franz-go`（纯 Go、幂等 producer、`kgo` 语义完整），Redis 客户端 `github.com/redis/go-redis/v9`；测试容器候选 `testcontainers-go/modules/redis` 与 `modules/kafka`（或 `modules/redpanda`）。新增依赖仅用于本 feature 范围（Charter XIII 由 spec 批准 + [adr.md](adr.md) 记录）。

**Storage**: PostgreSQL 为唯一权威（Charter III）。新增对象在 `migrations/000015_event_infrastructure.sql`（纯增量 DDL，编号在 `000014` 之后；见表清单见 [data-model.md](data-model.md)）：`outbox_events`、`consumer_progress`、`consumer_inbox`、`consumer_versions`、`consumer_quarantine`、`event_ops_audit`、`event_system_state`。Redis 仅承载缓存/限流/临时协调（非权威）；Kafka 仅承载事件投递（非权威）；两者丢失不得使权威状态不可恢复。

**Testing**: 分层（FR-28，细则 [verification.md](verification.md)）——Unit（无外部中间件）；Integration 分组件：PostgreSQL（沿用 `-tags integration`）/ Redis（新 tag）/ Kafka（新 tag）；Contract（事件信封、schema 版本、消费者兼容）；E2E（充值/提现核心流）；Fault Injection 与 Performance 独立运行。命令设计：`make test`、`make test-race`、`make test-integration` 保持现状；新增 `test-contract`、`test-integration-redis`、`test-integration-kafka`、`test-e2e`、`test-fault`、`test-perf`（实现批次添加，本文定义触发与预算方法）。本地编排：compose 增加 `redis`、`kafka`（KRaft 单节点，实现阶段固定镜像 tag），以 profile 保持 PG-only 基线可运行。

**Target Platform**: Linux server；单部署、单链（沿用 v1 范围）；本地确定性环境 Anvil + PostgreSQL + Redis + Kafka + Docker Compose（Charter X）。

**Project Type**: backend monorepo（模块化单仓）；新增 `internal/events`、`internal/cache`、`internal/ratelimit` 三个包；新增 3 个子命令；`serve` 扩展；一个迁移；compose/README 后续批次更新。

**Performance Goals**: 未实测，一律标待测。对照基准（PG-only vs 全栈）与采集口径见 [verification.md](verification.md) 与 [adr.md](adr.md)；本 plan 不编造 p95/p99/吞吐/追赶时间目标值。

**Constraints**: 至少一次投递 + 幂等处理；不宣称跨系统恰好一次；配置 fail-closed（无效/缺失关键组合拒绝启动）；PG 权威不依赖缓存/通道；`MUST NOT` 引入 Debezium/Kubernetes/多链；不重定义 002–011/PB 语义；迁移纯增量、已应用不重写；容量保护为硬约束；日志/事件不含密钥或原始签名材料；无外部调用发生在持有 PG 锁或事务内（发布在领取事务之外）。

**Scale/Scope**: 单链；topic 1 个、分区初始 6（理由与校准方法见 research R5，标初始值）；消费者名少量（参考消费者 + 下游接入位）；事件目录 v1 见下；无 UI。

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Pre-Phase-0（2026-09-24，against Constitution 1.1.0）**：

- **I（资金正确性优先）**: PASS — Outbox 同事务提交使「已提交状态必有事件」；事件身份 UNIQUE 防重复；容量保护拒绝新写入而非丢事件；修订不触发付款；数值路径无一浮点。
- **II（幂等）**: PASS — 发布器至少一次 + 消费者 `consumer_inbox` UNIQUE + 版本守卫；PG 持久层而非内存；重复处理收敛。
- **III（PG 权威）**: PASS — Redis/Kafka 均为非权威载体，丢失可重建；消费者进度有 PG 载体；缓存不覆盖权威。
- **IV（重组感知）**: PASS — 修订事件携带旧身份/新 canonical 或 Orphaned/原因/恢复版本，追踪 006 语义；Orphaned 不得作为 canonical 事实被消费。
- **V（显式状态机）**: PASS — `outbox_events` 发布状态机与隔离/重放状态显式 CHECK；消费者版本/隔离转换显式。
- **VI（事务边界/禁止双写）**: PASS — 业务转换 + Outbox 同事务；发布在事务外且仅针对已提交行；无「先写库再发布」路径。
- **VII（nonce 并发）**: PASS（边界）— 本 feature 不触碰 nonce 分配；事件重复投递不得触发新 nonce/签名/广播（FR-05），由门禁继承与消费边界保证。
- **VIII（密钥隔离）**: PASS — 签名边界不动；事件载荷不含私钥/签名材料；`internal/events` 不 import signer 密钥材料。
- **IX（失败路径一等）**: PASS — 故障矩阵、发布/消费重试分类与上限、隔离与重放、追赶再故障、容量溢出处置均有定义；重试有界（见 Complexity Tracking 对「发布重试次数不丢弃」的显式论证）。
- **X（确定性本地测试）**: PASS — compose 增加 Redis/Kafka，全场景本地可复现，不依赖公网测试网。
- **XI（不变量测试）**: PASS — 关键不变量（原子性、幂等、版本、修订、容量、缓存陈旧）均有 Integration/Contract/E2E/Fault 层设计；真实中间件而非 mock 替代。
- **XII（观测）**: PASS — FR-24 指标/日志清单落 [verification.md](verification.md)；指标不是事后补充。
- **XIII（简单性优先分布）**: PASS（有依据）— Redis/Kafka 是已批准规格与章程「架构演进」步骤的载体；ADR 记录必要性、收益假设、复杂度/故障/运维成本与对照基准；不引入 Debezium/K8s；抽象仅限真实边界。
- **XIV（小块、规格驱动）**: PASS — 本 plan；范围由 spec Non-Goals 围栏；不进入 tasks/implement。

**Post-Phase-1 re-check（2026-09-24）** — 设计后复核：无新增违规。附加说明：

- 新增的载体（outbox/consumer 五表 + 审计表 + 系统状态表）均为规格要求在 PostgreSQL 中的最小持久载体，对应 FR-07/08/13/14/15/20，不引入额外抽象层；`event_system_state` 为单行 cutover/维护标记，可裁剪为配置，若实现阶段证明无必要可去（记录于 Deferred）。
- 发布重试「次数不设丢弃上限、速率与单次时长有界、永久阻塞可见可审计」的设计经 Complexity Tracking 论证，不视为 IX 违规（丢弃事件才违反 I/VI/IX）。
- Redis/Kafka 引入不构成 XIII 违规：spec 与 PD-3 已批准范围，ADR 承担理由与基准；实现前不宣称收益。
- 无 Complexity Tracking 之外的例外；无原则被削弱。

## Inherited rulings（继承裁决；只消费，不重开）

| ID | 裁决要点（引自 spec，plan MUST NOT 变更） | 本 plan 载体 |
|---|---|---|
| PD-1（2026-09-23） | 限流失效时新提款创建拒绝 + 明确可重试错误；存量资金流程按原门禁继续；查询可回源；RPC 保持有界控制，无法维持则安全暂停续跑；不批准资金写入替代限流 | D6；[contracts/redis.md](contracts/redis.md) §3；FR-18 |
| PD-2（2026-09-23） | 停新保在途 + 预留恢复容量；链上已发生充值不得拒绝，无法安全持久化则从可靠进度暂停、恢复后补扫；绝不静默丢弃 | D7；[contracts/capacity.md](contracts/capacity.md)；FR-20 |
| PD-3（2026-09-23） | Kafka 保持 013 范围；不预判收益；ADR 写明理由/PG-only 替代/成本并设计可比基准；调整范围须另行提交用户决定 | [adr.md](adr.md) ADR-013-01/02；D9 |
| PD-4（2026-09-24） | 人工重放授权操作员执行 + 记录范围/操作审计；不要求第二人审批；受持久幂等与门禁约束；不得重执行链上付款或创建新提款意图；与自动重试区分 | D3；[contracts/consumer.md](contracts/consumer.md) §6；FR-14 |
| FR-28（2026-09-24） | 测试分层且各层独立；完整 Fault/Perf 不阻塞普通 PR；受影响资金安全回归仍须运行；Debezium 非运行依赖 | D10；[verification.md](verification.md) §3–4 |

**OQ 消费**：OQ1–OQ3、OQ5 由本 plan 机制决策关闭（research R5/R6/R10/R11/R15）；OQ4/OQ6 规格边界已锁定（FR-14/15、FR-07/11），机制在本 plan 设计（R9/R4）。

## Design spine

### D1 — 同事务 Outbox、事件身份、目录、上线衔接（FR-07/FR-09/FR-10；重点 1）

- **原子边界**：新增 `internal/events.Append(ctx, tx, Event)`，只接受调用方已在进行的 PostgreSQL 事务；所有目录内业务转换与其事件在同一事务提交（提交成功同可见、回滚同不可见）。禁止任何「业务提交后再补写事件」的调用形态。集成点按上游模块枚举：004 观察生成/状态转换、005 确认转换、006 修订与复活、007 提款请求接收、011 执行关键状态转换（细节见 [data-model.md](data-model.md) §4 集成点清单）。上游写入函数需要在其既有事务内调用 `Append`（实现批次改动各模块，但本 plan 只列点）。
- **不能补发路径的排除**：构造上不存在「已提交业务状态而必需事件无法补发」——事件行与业务状态同事务，要么同成要么同败；额外加两层兜底：(a) 每个集成点的 integration 测试做「提交/回滚原子性探针」（提交必有行、回滚必无行）；(b) `events-audit` 周期对账任务按 `source_kind/source_id/source_version` 比对已提交转换与 outbox 行，发现缺口即告警（检测兜底，不做静默补写；修复走操作员审计路径）。**MUST NOT** 用触发器或 CDC 替代显式 `Append`（Debezium 非依赖，PD-3/R1）。
- **事件身份**（[contracts/events.md](contracts/events.md) §2）：两类自然键 + 部分 UNIQUE。`evm_log` 类键 = `(chain_id, block_hash, tx_hash, log_index)`；`business_object` 类键 = `(aggregate_type, aggregate_id, aggregate_version)`。`event_id` 为确定性派生的传输身份（自然键 → UUIDv5）。写入用 `ON CONFLICT ... WHERE payload_hash = excluded.payload_hash`：同身份同内容 = 幂等 no-op；同身份异内容 = 拒绝事务 + 告警指标（不静默覆盖）。
- **单调版本语义**：`aggregate_version` 是**发射流内**按对象从 1 连续的版本（不是上游表版本直引）——避免旧数据上线后消费者看到人为版本缺口；上游状态/版本上下文放载荷（如 `source_state`、`policy_version`、`recovery_version`）。同对象事件跨分区/重放乱序时，消费者以版本守卫收敛；跨对象全局顺序不假定（FR-10）。
- **事件目录 v1（OQ6）**：见下表；FR-07 五域全覆盖（生成/转换、确认、修订、请求接收、执行关键转换），目录可扩展、命名规则 `<domain>.<object>.<fact>`、`schema_version` 从 1 起（FR-12 兼容规则见 D4）。
- **迁移与回退**：`000015_event_infrastructure.sql` 纯增量；down 仅用于本地/回滚窗口（drop 新表，已发布内容不回收）。回退程序：停止发布器/消费者 → 盘点 `pending|blocked` 事件（导出或先排空）→ 停业务写入窗口内执行 down → 移除接线。诚实声明：**只停事件发射而业务继续**不构成合规回退（会违反 FR-07），回退必须是受控窗口内的完整特性回退。
- **旧数据上线衔接**：不伪造历史事件。`event_system_state` 记录 `cutover_at`；首条发射事件版本从 1 起，消费者以「首见版本即基线」初始化，避免旧数据造成假缺口；下游需要历史状态的，用只读快照导出（操作员流程，见 quickstart）初始化后再消费增量。修订语义只对 cutover 后事实生效；旧对象在 cutover 后的首次转换正常发射。

**事件目录 v1**：

| # | 事件类型（v1） | 身份类 | 来源 | 覆盖 FR |
|---|---|---|---|---|
| 1 | `deposit.observation.created` | evm_log + 对象版本 | 004 观察生成 | FR-07、FR-09 |
| 2 | `deposit.observation.status_changed`（含 orphaned） | 对象版本 | 004/006 状态转换 | FR-07、FR-11 |
| 3 | `deposit.observation.reinstated`（006 FR-08 复活） | 对象版本 | 006 | FR-11 |
| 4 | `deposit.confirmation.confirmed` | 对象版本 | 005 确认转换 | FR-07 |
| 5 | `deposit.revision.applied`（旧身份 + 新 canonical/Orphaned + 原因 + recovery_version） | 对象版本（携带旧身份） | 006 修订 | FR-11 |
| 6 | `withdrawal.request.received` | 对象版本 | 007 接收（Accepted 语义不变） | FR-07、FR-05 |
| 7 | `withdrawal.execution.state_changed`（011 关键转换，含终态） | 对象版本 | 011 | FR-07 |
| 8 | `withdrawal.execution.revised`（链上事实修订） | 对象版本 | 011/010 修订 | FR-11 |

载荷最小化：链身份、业务身份、版本、状态/原因、恢复版本、occurred_at；不含密钥、原始签名字节、调用方凭据。

### D2 — 发布器、投递确认、崩溃恢复、顺序边界、追赶（FR-08/FR-21；重点 2）

- **进程**：`event-publisher`。领取：单事务 `SELECT ... WHERE publish_state='pending' AND next_attempt_at<=now() ORDER BY id LIMIT $batch FOR UPDATE SKIP LOCKED` → 写 `claim_owner/claim_expires_at` → 提交；**发布在事务外**；Kafka ack 后第二个事务 `UPDATE ... SET publish_state='published', published_at=now() WHERE id=$1 AND claim_owner=$2`；崩溃/租约过期重新领取 → 重复发布可发生（至少一次），消费者幂等吸收。多实例经 SKIP LOCKED + 租约天然分片；无 Redis 锁参与（Redis 故障不影响发布器正确性）。
- **投递确认**：producer `acks=all`、`enable.idempotence=true`、`max.in.flight.requests.per.connection≤5`（幂等 producer 下保持分区内顺序）、`delivery.timeout.ms` 有界、按批 `ProduceSync` 等待确认；确认只代表 broker 按 topic 策略持久化，**不宣称端到端恰好一次**。永久类失败（序列化/无效载荷）→ `publish_state='blocked'` + 告警 + 操作员解阻/重放；瞬时类失败（broker 不可达/超时/限流）→ 释放领取、指数退避 + 抖动重排 `next_attempt_at`。
- **顺序边界**：分区键 = `aggregate_type:aggregate_id`，同对象事件落同一分区；分区内顺序是**尽力优化**（重领取/重试/多实例下重复可交错），消费者版本守卫才是正确性边界（[contracts/outbox-publisher.md](contracts/outbox-publisher.md) §2）。
- **追赶**：Kafka 恢复后排空；批量/并发/轮询均配置有界；`outbox_pending_count`、`outbox_pending_oldest_age_seconds`、发布失败分类计数见 verification.md。
- **Topic 设计（OQ1）**：单 topic `txharbor.events.v1`；分区初始 6（初始值，理由：追赶并行与 rebalance 余量；校准方法见 research R5）；消费组 `txharbor.<consumer_name>`；offset 双载体（D3）。

### D3 — 消费者：持久幂等、版本守卫、重试、隔离、重放、进度（FR-13/14/15；重点 2/3）

- **保证范围**：至少一次投递 + 幂等处理；同一事件有效应用次数恰好一次（在消费者 PG 效果内）。不宣称跨系统恰好一次。
- **应用事务（T4）**：`INSERT consumer_inbox(consumer_name,event_id,...) ON CONFLICT (consumer_name,event_id) DO NOTHING` → 0 行 = 已应用，跳过；同时版本守卫 `consumer_versions`：仅当 `aggregate_version > max_version` 应用，`==` 幂等跳过，`> max+1` = 版本缺口 → 有界等待（配置窗口）后仍缺 → 隔离 `version_gap` + 告警（不得静默跳过、不得无限阻塞分区）；效果写入与 inbox/version/progress 同一事务。
- **重试**：可重试类（瞬时 PG/依赖）有界退避；超上限或不可重试类 → `consumer_quarantine`（持久、审计、告警），跳过该事件继续分区进度；修复后重放幂等。毒事件不阻塞其他事件或分区进度。
- **进度**：`consumer_progress(consumer_name, topic, partition, next_offset)` 在效果事务内推进（高水位）；Kafka offset 在处理提交后异步提交；重启/再均衡以 PG 进度为准（无 PG 记录时用 Kafka committed offset），重投递由 inbox 去重。lag 与追赶时间可观测。
- **rebalance/重启**：每分区顺序处理；再均衡后从 PG 进度续传；跨进程/实例不依赖内存。
- **人工重放（OQ4/PD-4）**：`events-admin replay`——授权操作员执行，记录 `event_ops_audit(operation_id UNIQUE, operator, scope, reason, result)`；重放 = 重新投递或重新处理历史事件，必经 inbox/版本守卫，不绕过门禁；**不得**重执行链上付款或创建新提款意图；与自动重试在审计与指标上明确区分。解阻（`blocked`/隔离）同走审计。
- **未知 schema 版本**：fail-closed 隔离 + 告警（FR-12）。
- **参考消费者**：`event-consumer`（`internal/events/refconsumer`）用 inbox + 版本表证明「恰好一次效果」，用于验收证据；明确标注**非生产账本、非权威**，演示其验证边界（FR-16）。

### D4 — 修订事件与版本兼容（FR-11/FR-12）

修订事件（目录 #5/#8）表达旧事件/旧区块身份、新 canonical 状态或 Orphaned 处置、原因、`recovery_version`；幂等；不触发新提款意图/发送；消费者收敛后不再把 Orphaned 当有效事实；006 FR-08 同 `block_hash` 复活与「新来源观察」区分。兼容规则：新增可选字段同版本可用、消费者忽略未知字段；破坏性变更必须新版本 + 兼容窗口；未知版本 fail-closed。契约细节 [contracts/events.md](contracts/events.md) §3–4。

### D5 — Redis 缓存（FR-17/FR-23；重点 4）

- **只缓存非权威读模型**：决策路径（执行、门禁、对账、修订判定）**永不读缓存**（继承 011 「投影不参与决策」原则）；查询类接口按需缓存展示数据，响应标注新鲜度（`freshness`），陈旧不可确认时明确标注。
- **键与失效**：键带 `cache_epoch`（命名空间版本）与对象版本；权威转换的 outbox 事件驱动同进程/独立失效器删除受影响键范围（事件即失效信号，不新增双写）；TTL 兜底。Redis 重启/清空 → epoch 变更使旧值不可达；**不提供已失效陈旧值**。
- **故障与恢复**：Redis 不可用/miss → 直读 PG；回源有界保护（singleflight + 并发信号量，防止击穿压垮 PG）；恢复后惰性重建/受控预热，平滑恢复，不瞬间无界放开。
- Redis 锁（如有）仅用于效率（防击穿），**MUST NOT** 作为资金状态/幂等/门禁的唯一载体（FR-02）。

### D6 — 分布式 API/RPC 限流与失效处置（FR-18/FR-19；重点 4）

- 限流器实现于 Redis（令牌桶/滑动窗口，Lua 原子），按接口类配置速率与突发；**限流不构成授权**，认证/授权/幂等/门禁照旧执行。
- **失效处置（PD-1）**：Redis 限流不可用时，`POST /withdrawals`（新提款创建）fail-closed 拒绝 + 明确可重试错误（HTTP 503/429 + `Retry-After`，错误分类沿用既有 taxonomy）；充值观察、确认、重组恢复、已接受提款的执行继续按原门禁；查询回源 PG；**不引入资金写入的替代限流**。
- **RPC 有界控制**：出站 RPC 的既有每进程有界控制（超时、重试上限、并发上限）不依赖 Redis；分布式 RPC 预算为叠加治理项。若某类调用只能靠分布式预算维持有界性，Redis 故障时安全暂停该类调用、恢复后续跑；不得借故障跳过分类/完整性校验（FR-19）。
- **恢复**：平滑重建（梯度放开 + 预热），不瞬间全放开；拒绝计数/降级状态可观测。

### D7 — 容量保护与「停新保在途」（FR-20；重点 5）

- **可观测**：`outbox_pending_count`、`outbox_pending_oldest_age_seconds`（按事件类）；由发布器/对账任务查询 PG 部分索引得出；Redis 不参与。
- **配置边界**：`soft_limit`（开始拒绝可控制新资金写入）、`hard_limit`（停止新 intake）、`reserve`（为在途与关键事件预留）、`retention`、`max_shutdown_window`（可承受停机时间）。全部为待测数值，plan 给**公式与测量方法**（research R13）：`reserve ≥ max_events_per_admitted_operation × max_in_flight_operations`（实测每次操作的事件写放大与并发）；`soft_limit = reserve + measured_drain_rate × target_drain_window`（目标窗口为配置项/业务裁决）；`hard_limit = soft_limit + reserve`。配置 fail-closed：不满足 `soft<hard`、`reserve>0` 拒绝启动。
- **行为**：`pending < soft` 正常；`soft ≤ pending < hard` 拒绝可控制的新资金写入（可重试错误）+ 降级非关键功能；链上观察/确认/修订与在途提款继续。接近/达到 hard：若无法安全持久化，从 003/004 可靠进度暂停链上处理、恢复后**补扫**（不跳过观察）；在途提款收尾按既有暂停/对账协议，不新建意图。
- **不矛盾论证**：容量保护在**接纳新可控制工作之前**判定；每个已接纳单元「业务状态 + 必需事件」原子提交，因此一旦接纳，事件行必然可写；`reserve` 依实测的「单次操作最大事件数 × 在途并发」设置，保证在途完成；到达上限的只会是**不可拒绝**的链上事件（此时暂停处理而非拒绝事实）与已被拒前的存量，故「在途可完成」与「Outbox 已满」不冲突。形式化不变量与测试见 [contracts/capacity.md](contracts/capacity.md) §2、[data-model.md](data-model.md) §6。

### D8 — 故障矩阵实现映射（FR-04/05/06）

| 矩阵操作 | 实现载体 | 依赖判定 |
|---|---|---|
| 充值处理 | 002/003/004 不变 + Outbox 同事务；Redis 只读缓存旁路；Kafka 故障只积压 | 不依赖 Redis/Kafka |
| 确认与重组恢复 | 005/006 门禁读 PG；修订入 Outbox | 不依赖事件送达判完成 |
| 提款创建（接收） | 007 receive-only 不变；限流失效 fail-closed；容量软边界拒绝可控新写入 | 事件送达不属于接收语义 |
| 已有提款执行 | 008–011 门禁与发送前重验（010/011 已有） | 事件不得触发新发送 |
| 查询 | 降级直读 PG + 新鲜度标注；投递状态降级信息 | 不返回陈旧财务权威 |
| 事件订阅 | Outbox + 发布器 + 消费者；故障期保留积压 | 重复/乱序由幂等契约承接 |
| 非关键功能 | 降级开关（配置/健康状态驱动） | 不阻塞关键路径 |

### D9 — 观测、价值证明与 ADR（FR-24/FR-25；重点 6/7）

指标/日志口径与对照基准协议独立成文 [verification.md](verification.md)；引入理由、PG-only 替代、复杂度/故障/运维成本、基准设计、范围保持与 Debezium 排除独立成文 [adr.md](adr.md)。本 plan 只固定：指标来源（发布器/消费者/容量查询/HTTP 中间件/Redis 客户端统计）、基准的相同负载与故障场景要求、报告模板字段、以及「无实测不宣称 / 未测数值标待测」的表述纪律。

### D10 — 测试分层与 CI 成本（FR-28；重点 7）

分层与触发见 [verification.md](verification.md) §3–4：Unit 无中间件；Integration 按 PG/Redis/Kafka 分组件独立运行；Contract（无中间件，`make test-contract` 独立入口）验证事件格式/版本/消费者兼容；E2E 核心充提流；Fault Injection 与 Performance 独立运行、不在普通 PR 触发；受影响资金安全回归（幂等、门禁、重放断言）在普通 PR 仍运行。预算给方法（同 runner 基线的倍数 + 硬上限来自 CI timeout），不编造分钟数。本步不改 ci.yml，只定义实现批次的接入要求。

## Seven-key closure map（任务重点闭合项 → 载体）

| # | 重点 | 载体 |
|---|---|---|
| 1 | Outbox 同事务边界/事件身份/单调版本/目录/旧数据/迁移回退 | D1；[contracts/events.md](contracts/events.md)；[data-model.md](data-model.md) §2/§4/§8；research R1–R4/R14 |
| 2 | 发布器投递确认/崩溃/重复乱序/顺序边界/追赶；消费者去重/进度/重试/隔离/恢复；保证范围表述 | D2/D3；[contracts/outbox-publisher.md](contracts/outbox-publisher.md)；[contracts/consumer.md](contracts/consumer.md)；research R5–R9 |
| 3 | 修订版本兼容/人工重放授权审计/自动重试区分/上游账本边界 | D3/D4；[contracts/events.md](contracts/events.md) §3–4；[contracts/consumer.md](contracts/consumer.md) §6；FR-16 边界声明 |
| 4 | Redis 回源/分布式限流/RPC 故障/已裁决矩阵 | D5/D6；[contracts/redis.md](contracts/redis.md)；research R10–R12 |
| 5 | 容量模型/提前停收/预留/补扫/「在途可完成」不矛盾/阈值方法 | D7；[contracts/capacity.md](contracts/capacity.md)；research R13 |
| 6 | ADR：必要性/收益假设/成本/对照基准/范围保持/Debezium 不引入 | [adr.md](adr.md)（ADR-013-01/02 + 基准设计） |
| 7 | 观测与验证：指标口径/关 Redis·Kafka 保 PG 验收/FR-28 分层/PR 触发与预算方法 | [verification.md](verification.md)；D9/D10 |

## Project Structure

### Documentation (this feature)

```text
specs/013-reliable-event-infrastructure/
├── plan.md              # This file (/speckit.plan output)
├── spec.md              # approved spec (untouched)
├── research.md          # Phase 0 output (R1–R18 decisions)
├── data-model.md        # Phase 1 output (tables, state machines, txn catalog, capacity invariants)
├── quickstart.md        # Phase 1 output (validation/drill guide; design-only)
├── adr.md               # Phase 1 companion (FR-25/PD-3; template has no ADR section)
├── verification.md      # Phase 1 companion (FR-24/FR-28 observability, drills, test layering)
├── contracts/
│   ├── events.md            # envelope, identity, catalog, version compat, revision semantics
│   ├── outbox-publisher.md  # outbox states, claim protocol, ordering, crash matrix, retry classes
│   ├── consumer.md          # consumer contract, dedup/version/retry/quarantine/progress/replay
│   ├── redis.md             # cache + distributed rate limiting contract and failure policy
│   └── capacity.md          # capacity guard contract, formula, config bounds, non-contradiction proof
├── checklists/
│   └── requirements.md  # specify/clarify artifact (consumed, not rewritten)
└── tasks.md             # Phase 2 output (/speckit.tasks — NOT created here)
```

### Source Code (repository root)

```text
internal/events/                 # NEW: outbox + publisher + consumer runtime + catalog
├── append.go        # Append(ctx, tx, Event): insert-first, conflict detection, payload hash
├── catalog.go       # event types v1, schema versions, identity/version derivation helpers
├── outbox.go        # outbox reads/gauges, state machine guards, retention/audit queries
├── publisher.go     # claim/lease, bounded batch+concurrency, publish, ack-mark, retry classes
├── consumer.go      # consume loop: inbox/version/progress transaction, bounded retry, gap policy
├── quarantine.go    # quarantine store + operator unblock/replay primitives (audited)
├── refconsumer.go   # reference consumer (evidence only; explicitly non-authoritative)
└── *_test.go
internal/cache/                  # NEW: cache-aside client, epoch namespacing, bounded fallback
├── cache.go
└── *_test.go
internal/ratelimit/              # NEW: distributed limiter + failure policy (PD-1)
├── limiter.go
├── policy.go        # per-interface-class policy, fail-closed decisions, smooth recovery
└── *_test.go
internal/app/
├── eventpublisher.go   # `event-publisher` subcommand loop wiring
├── eventsadmin.go      # `events-admin` operator subcommand (quarantine/replay/unblock/audit)
├── eventconsumer.go    # `event-consumer` reference consumer subcommand (evidence)
├── serve.go            # EXTEND: cache + limiter middleware wiring, degraded-status surfaces
└── *_test.go
cmd/txharbor/main.go     # EXTEND: dispatch event-publisher / events-admin / event-consumer
internal/config/config.go# EXTEND: TXHARBOR_EVENTS_*/REDIS_*/KAFKA_*/CACHE_*/RATELIMIT_* fail-closed
internal/metrics/        # EXTEND: outbox/consumer/cache/rate-limit metrics on existing registry
internal/health/         # EXTEND: dependency health (Redis/Kafka) as non-authoritative signals

migrations/
└── 000015_event_infrastructure.sql   # 7 new tables, pure additive DDL (data-model.md)

compose.yaml             # IMPLEMENTATION BATCH: add redis + kafka (KRaft) under a profile,
                         # keeping base PG+Anvil as the PG-only baseline
Makefile                 # IMPLEMENTATION BATCH: test-contract, test-integration-redis/kafka, test-e2e/fault/perf
```

**Structure Decision**: 沿用 `internal/<domain>` 单包一域布局（009/011 先例）与子命令接线模式；`internal/events` 拥有 outbox/发布/消费/隔离/审计，不 import 上游 writer 包与 RPC/dial 包（import 边界测试）；上游集成只经同一事务内的 `Append` 调用与只读查询；`internal/cache`、`internal/ratelimit` 独立可测且不持有权威状态。无新监听器：`events-admin`/`event-publisher`/`event-consumer` 均为子命令循环。

## FR → design → verification mapping

| FR | Design carrier | Verification (future) |
|---|---|---|
| FR-01 PG 唯一权威 | D1/D5；非权威载体边界；重建路径 | V-BASE、V-FAULT-PG-only、[verification.md](verification.md) §2 |
| FR-02 Redis/Kafka 用途限定 | D5/D6/D2；[contracts/redis.md](contracts/redis.md) §1 | import/使用边界测试；无权威写入断言 |
| FR-03 不引入其他基础设施、不重定义上游 | 范围声明；[adr.md](adr.md) | review gate；compose/deps diff |
| FR-04 故障矩阵 | D8 表；[verification.md](verification.md) §2 五态演练 | V-FAULT-MATRIX（独立 Fault 层） |
| FR-05 门禁继承、重复投递不得触发新效果 | D3/D8；consumer 契约 §4；FR-16 边界 | V-IDEMPOTENCY、V-FAULT-MATRIX、E2E |
| FR-06 故障不变量（0 重复等） | D7/D8；[contracts/capacity.md](contracts/capacity.md) §4 | V-FAULT、V-E2E 断言组 |
| FR-07 同事务 Outbox + 目录 | D1；`Append`；目录表；集成点清单 | V-ATOMICITY 原子性探针；catalog conformance |
| FR-08 至少一次发布、崩溃恢复、有界尝试 | D2；[contracts/outbox-publisher.md](contracts/outbox-publisher.md) | V-PUBLISHER（崩溃点矩阵、多实例） |
| FR-09 事件身份 + UNIQUE + 冲突告警 | D1；[contracts/events.md](contracts/events.md) §2 | 23505/冲突探针；冲突告警断言 |
| FR-10 版本顺序与链身份 | D1/D3；版本守卫；链身份字段 | 乱序/缺口/旧覆盖新测试 |
| FR-11 重组修订 | D4；修订事件载荷规则 | V-REVISION（含 block_hash 复活） |
| FR-12 schema 版本兼容 | D4；[contracts/events.md](contracts/events.md) §4 | Contract 层兼容矩阵；未知版本 fail-closed |
| FR-13 消费者持久幂等 | D3 T4；inbox UNIQUE | 重复投递/重启/rebalance 后恰好一次效果 |
| FR-14 有界重试/隔离/重放/自动重试区分 | D3；隔离表；events-admin | V-RETRY-QUARANTINE；PD-4 审计断言 |
| FR-15 进度持久与续传、lag 可观测 | D3；`consumer_progress`；指标 | 重启续传、offset 提交失败重放 |
| FR-16 入账边界声明 | D3 参考消费者；[verification.md](verification.md) §5 | 证据声明检查 |
| FR-17 缓存查询行为 | D5；epoch/失效/新鲜度 | V-CACHE（陈旧读取 0、击穿有界） |
| FR-18 PD-1 限流失效 | D6；[contracts/redis.md](contracts/redis.md) §3 | V-RATELIMIT（拒绝新创建 + 存量继续） |
| FR-19 RPC 降级不破坏契约 | D6；既有分类继承 | RPC 分类/完整性校验不被跳过断言 |
| FR-20 PD-2 容量保护 | D7；[contracts/capacity.md](contracts/capacity.md) | V-CAPACITY（停新保在途、补扫） |
| FR-21 发布器恢复排空 | D2 | V-CATCHUP（恢复排空、再故障） |
| FR-22 消费者追赶与再故障 | D3 | V-CATCHUP |
| FR-23 追赶期缓存/限流重建 | D5/D6 | V-RECOVERY（平滑、不提供陈旧值） |
| FR-24 观测 | D9；[verification.md](verification.md) §1 | 指标存在性/标签/脱敏检查 |
| FR-25 对照基准与 ADR | [adr.md](adr.md)；§基准设计 | 报告产出与表述纪律检查 |
| FR-26 故障演练验收 | [verification.md](verification.md) §2；quickstart | V-DRILL（双故障演练证据） |
| FR-27 范围留白纪律 | 本 plan 全文；Deferred 表 | plan review |
| FR-28 测试分层与 CI 成本 | D10；[verification.md](verification.md) §3–4 | 分层可独立运行验证；PR 触发矩阵 |

## SC → verification mapping

| SC | 验收方法（详见 [verification.md](verification.md)） |
|---|---|
| SC-01 矩阵一致率 100%、门禁绕过 0 | V-FAULT-MATRIX 五态 × 七类断言 |
| SC-02 双故障/恢复后 0 重复意图/付款/孤儿入账/权威丢失 | V-DRILL 断言组 + E2E |
| SC-03 状态↔事件 100%/0；重复投递财务重复 0 | V-ATOMICITY + 参考消费者 inbox 基数断言 |
| SC-04 有效应用 1 次、旧覆盖新 0、缺口静默跳过 0 | V-IDEMPOTENCY + 乱序/缺口用例 |
| SC-05 毒事件 100% 隔离可审计可重放、无界重试 0、静默丢弃 0 | V-RETRY-QUARANTINE |
| SC-06 进度 100% 持久恢复、重放重复效果 0 | V-PROGRESS |
| SC-07 修订后 Orphaned 误用 0、修订幂等 100%、新付款 0 | V-REVISION |
| SC-08 陈旧财务权威 0、无限制放行 0、恢复后陈旧读取 0 | V-CACHE + V-RATELIMIT |
| SC-09 停机 0 丢失、可观测 100%、PD-2 行为、充值 0 拒绝、排空有界 | V-CAPACITY + V-CATCHUP |
| SC-10 再故障 0 丢失/0 重复、进度可续 100% | V-CATCHUP |
| SC-11 对照报告 100% 产出、未测不宣称 | 基准协议 + 报告检查 |
| SC-12 证据覆盖矩阵、边界声明、外部保证 0 | [verification.md](verification.md) §2/§5 证据审查 |

## Upstream inputs & downstream dependencies

**上游输入（只读消费，不重定义）**：D1 002/003 链视图与日志三元组身份；D2 004 观察语义与进度；D3 005 确认门禁与策略版本；D4 006 修订/复活与恢复版本；D5 007 receive-only 与幂等契约；D6 008/009/010 执行链门禁；D7 011 意图/投影与执行事件；D8 PB 授权 scope；D9 本 feature 自建载体；D10 compose（实现批次扩展）。集成点（`Append` 调用位）在 [data-model.md](data-model.md) §4 逐表列出；上游实现证据沿用各自记录，不在本轮核验。

**下游依赖（handoff）**：下游账本消费者按事件身份/版本/投递语义实现幂等消费并声明验证边界（FR-16）；006/008/009/010/011 语义不被事件解释为许可（FR-05）；007 Accepted 语义不变；运维获得容量/隔离/重放/追赶路径（quickstart + events-admin）；T000-P 保持 OPEN。

## Blockers

**无**。已裁决边界（PD-1–PD-4、FR-28、章程 1.1.0）均可在本设计中满足，未发现需要修改 spec 的阻塞点。残余事项（非阻塞）记录于 Deferred：实测阈值、基准结果、Kafka 范围调整（如基准支持，须另行提交用户裁决，PD-3）、客户端库与容器镜像的可用性验证（实现批次）、`event_system_state` 是否可裁剪为配置。若实现批次发现任一已裁决边界不可满足，按章程与任务规则回报 orchestrator，不自行变更裁决。

## Complexity Tracking

| Non-standard choice | Why needed | Simpler alternative rejected because |
|---|---|---|
| 发布重试按「速率/单次时长有界 + 永久阻塞可见可审计」，不设丢弃上限 | 已提交事件 MUST NOT 丢失（FR-08/I/VI）；Kafka 可长时间不可用（FR-20/21） | 设最大次数后丢弃 = 静默丢事件，违反 I/VI/IX；上界只对**尝试速率与时长**设限 |
| `outbox_events` 单表承载发布状态 + 容量观测（不拆表） | 容量保护、发布状态、审计需要同一行事实；减少双写/一致性面 | 拆 attempt 历史表增加写放大与运维面，但不增加正确性（Charter XIII） |
| `consumer_inbox` + `consumer_versions` 双载体 | inbox 证「事件级恰好一次效果」，versions 证「对象级版本单调与缺口」；二者查询模式不同 | 仅 versions 无法证明事件级去重；仅 inbox 无法判缺口/旧覆盖新 |
| Redis 缓存 + 分布式限流作为运行时依赖（新增 2 个中间件） | 已批准规格（013）与章程「架构演进」步骤；ADR 记录理由与基准 | PG-only 替代已评估（[adr.md](adr.md)）；缓存/限流收益假设待基准验证，范围调整另行裁决（PD-3） |
| PG 进度 + Kafka offset 双进度载体 | 进度不因 Kafka offset 丢失/重置而回退；可观测性不依赖 Kafka 管理面 | 仅 Kafka offset：管理面/重置风险且不可从 PG 重建；仅 PG 进度：再均衡 seek 依赖 PG 单点读（作为主载体，Kafka offset 为辅助） |

## Deferred items（显式，含归属）

| ID | Item | Owner |
|---|---|---|
| OQ1–OQ3/OQ5 | 已由本 plan 机制决策关闭（R5/R6/R10/R11/R15）；数值类输入待实测校准 | 实现/性能批次 |
| OQ4/OQ6 | 边界已锁定；机制见 D3/D4 与 contracts | 已设计 |
| 实测阈值 | soft/hard/reserve/retention、限流速率、退避/超时/追赶窗口、告警阈值 | 性能批次测量后校准（预算与方法见 verification.md） |
| Kafka 价值证明 | 同负载对照基准结论；范围调整须另行提交用户裁决（PD-3） | 性能批次 + 用户裁决 |
| 客户端/镜像 | franz-go、go-redis、testcontainers 模块、Kafka/Redis 镜像 tag 的可用性验证与固定 | 实现批次 |
| `event_system_state` | 若仅 cutover 标记可退化为配置项，实现阶段确认后裁剪 | 实现批次 |
| T000-P | 生产 provider 选型保持 OPEN，不宣称生产就绪 | 生产化轨道 |
