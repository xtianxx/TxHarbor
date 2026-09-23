# Phase 0 Research: 013 Reliable Event Infrastructure

**Feature**: 013-reliable-event-infrastructure | **Date**: 2026-09-24 | **Plan**: [plan.md](plan.md)

输入：`spec.md`（FR-01–FR-28、SC-01–SC-12、PD-1–PD-4、OQ1–OQ6）、章程 1.1.0、上游只读契约 002–011/PB、仓库现状（`migrations/000001`–`000014`、`internal/*`、`compose.yaml`、`Makefile`）。

产出规则：每条 = Decision / Rationale / Alternatives considered。阈值类：仅技术初始值（标注「初始值，测量后校准」）或纯测量方法/配置边界；业务阈值一律不编造。

---

## R1 — Outbox 载体：显式 `Append` API（拒绝触发器与 CDC/Debezium）

**Decision**: 新增 `internal/events.Append(ctx, tx, Event)`，所有目录内业务转换在同一 PostgreSQL 事务内调用；表 `outbox_events`（[data-model.md](data-model.md) Table 1）。不采用数据库触发器，不引入 Debezium/CDC。

**Rationale**: 显式 API 使「状态 + 事件」的原子边界可读、可测、可 review；触发器把载荷构造藏进 SQL，难以维护版本/身份派生；CDC（Debezium）是额外运行时依赖，spec 与 FR-28 明确其不是 013 运行依赖（PD-3 收口）。与 011 `execution_events`、006 `reorg_recovery_events` 的显式事件风格一致。

**Alternatives**: (a) AFTER 触发器自动写 outbox——排除，隐藏控制流且 SQL 内派生版本易漂移；(b) Debezium 读 WAL——排除，非批准依赖；(c) 业务代码提交后异步派生——排除，违反 FR-07/章程 VI（可能丢/晚事件）。

## R2 — 事件身份：两类自然键 + 部分 UNIQUE + payload_hash 冲突检测

**Decision**: `identity_kind ∈ {evm_log, business_object}`；`evm_log` 唯一键 `(chain_id, block_hash, tx_hash, log_index)`，`business_object` 唯一键 `(aggregate_type, aggregate_id, aggregate_version)`（部分索引）；`event_id` = 自然键的 UUIDv5 确定性派生（同事实同身份）；`payload_hash`（sha256 of canonical payload bytes）；写入 `INSERT ... ON CONFLICT (<键>) DO UPDATE SET ... WHERE outbox_events.payload_hash = EXCLUDED.payload_hash`，0 行 = 冲突 → 事务拒绝 + `events_identity_conflict_total` 告警。

**Rationale**: 满足 FR-09/章程 II；确定性身份使重试/重放天然同身份；payload_hash 使「同身份异内容」可检测（不静默覆盖）。`block_hash` 参与 EVM 身份（高度不单独作身份，003 契约）。

**Alternatives**: (a) 全局 UUID 随机主键——无法阻止同一事实重复插入；(b) 只靠 `event_id` 唯一——身份生成错误时静默放行；(c) 先查后插（SELECT then INSERT）——对并发不封闭；ON CONFLICT 更稳。

## R3 — `aggregate_version` 语义：发射流内连续版本（非上游版本直引）

**Decision**: 每个业务对象的 `aggregate_version` 从 1 起、在发射流内 +1 连续；上游版本/状态上下文放载荷字段（`source_state`、`policy_version`、`recovery_version` 等）。消费者对「首见事件」以该版本初始化基线。

**Rationale**: 旧数据上线（cutover）后，历史版本不存在于事件流；若直引上游版本，消费者会看到版本缺口（FR-10 禁止静默跳过）而误隔离。发射流连续版本让「缺口 = 真丢事件」，语义干净；上游事实仍可从载荷与 PG 追溯。

**Alternatives**: (a) 直引上游表版本号——cutover 假缺口/旧覆盖新判定混乱；(b) 每对象独立序列表——多一张表与锁竞争，收益仅是复用旧号（无消费者需要旧号）。

## R4 — 事件目录 v1、命名与 schema 表示（OQ6）

**Decision**: 目录 8 类事件（[plan.md](plan.md) 目录表；[contracts/events.md](contracts/events.md) §3）：`deposit.observation.created`、`deposit.observation.status_changed`、`deposit.observation.reinstated`、`deposit.confirmation.confirmed`、`deposit.revision.applied`、`withdrawal.request.received`、`withdrawal.execution.state_changed`、`withdrawal.execution.revised`。命名 `<domain>.<object>.<fact>`；信封 `schema_version` 整数从 1 起；`event_type` + `schema_version` 决定解析规则；载荷 JSONB，禁止密钥/凭据/原始签名字节；目录可扩展，新增类型不改变既有语义。

**Rationale**: 满足 FR-07 的「至少覆盖」与 FR-27 的「具体清单留 plan」；以「状态转换事实」而非「表变更」为粒度，避免实现耦合；修订单独成类（FR-11）便于消费者专门守卫。

**Alternatives**: (a) 按表 1:1 事件——泄露实现且转换语义分散；(b) 细分到每个字段变化——噪音与版本爆炸；(c) Protobuf/Avro 强 schema——引入注册中心（新基础设施），超出批准范围；JSONB + schema_version + Contract 测试足够。

## R5 — Topic/partition/key、消费组、offset 载体（OQ1）

**Decision**: 单 topic `txharbor.events.v1`；分区数初始 6（初始值，测量后校准：下界 = 期望消费者并行度，上界 = 单 broker 可承受且 rebalance 成本可接受；用基准的追赶时间与 PG 排空速率校准）；分区键 = `aggregate_type:aggregate_id`；消费组名 `txharbor.<consumer_name>`；offset 双载体：PG `consumer_progress` 为主、Kafka committed offset 为辅（见 R8）。

**Rationale**: 单 topic 简单、便于统一契约与工具；key 保同对象落同分区（顺序优化）；分区数可后续扩容但 key→分区映射会变，故消费者正确性不依赖分区顺序（版本守卫兜底，R7）。多 topic 需按域拆契约/权限/工具，收益（域隔离）当前消费者少且可被 quarantine 覆盖。

**Alternatives**: (a) 每域一 topic——运维面与契约面翻倍；保留为后续演进选项（版本化 topic 可新建而非原地改）；(b) 按事件类型分区——同对象顺序被打散，无收益；(c) 单分区——追赶无并行，恢复慢。

## R6 — 发布状态机与多实例竞争（OQ2）

**Decision**: `outbox_events.publish_state ∈ {pending, published, blocked}` + `claim_owner/claim_expires_at/attempt_count/next_attempt_at`。领取事务：`SELECT ... WHERE publish_state='pending' AND next_attempt_at<=now() ORDER BY id LIMIT $batch FOR UPDATE SKIP LOCKED` → 置 claim → 提交；事务外 `ProduceSync`（`acks=all`、幂等 producer、`max.in.flight≤5`）；ack 后事务：`UPDATE ... SET publish_state='published', published_at=now() WHERE id=ANY($ids) AND claim_owner=$owner`。瞬时失败 → 退避（base 1s、cap 60s、±20% 抖动——初始值，测量后校准）；永久类（序列化/契约校验失败）→ `blocked` + 告警 + `events-admin unblock/replay`。

**Rationale**: SKIP LOCKED 是 PG 上多实例领取的标准无锁竞争方案；租约使崩溃实例的领取可回收；「先提交领取，再发布」保证未提交行永不发布；重复发布由至少一次语义与消费者幂等吸收（FR-08）。计数不设丢弃上限的理由见 plan Complexity Tracking。

**Alternatives**: (a) `FOR UPDATE` 长事务含网络发布——持锁跨外部调用，违反 011 同款锁纪律、易堆积；(b) advisory lock 单发布器——牺牲并行与恢复速度；(c) Redis 锁——Redis 故障即停发，违反 FR-01/FR-21 不依赖 Redis。

## R7 — 消费者幂等载体与版本守卫、缺口策略（FR-13/FR-14）

**Decision**: 应用事务（T4）：inbox 插入（UNIQUE(consumer_name,event_id)）→ 0 行则跳过；版本守卫（`consumer_versions`）：>`max_version` 应用、`=` 跳过、`>max_version+1` 走缺口策略；效果与三行同事务。缺口策略：在配置等待窗口（初始 10s，测量后校准）内等待更早事件；仍缺 → 隔离（`failure_class='version_gap'`）+ 告警 + 分区进度继续（不无限阻塞）。重试：可重试类有界退避（base 500ms、cap 30s、上限 8 次——初始值，测量后校准），超限/不可重试 → 隔离。

**Rationale**: inbox 给出「事件级恰好一次效果」的可验证证据；versions 给出对象级单调与缺口检测，两者查询模式不同（plan Complexity Tracking）。隔离而非无限阻塞满足 FR-14/SC-05；隔离事件可重放且重放幂等（inbox）。

**Alternatives**: (a) 仅 Kafka 事务（read-process-write）——需 Kafka 事务跨 PG 写，做不到跨系统原子；不采用，也不宣称；(b) 仅内存去重——违反章程 II；(c) 缺口时阻塞分区——违反「不得无限阻塞其他事件或分区进度」。

## R8 — 进度载体、lag 与追赶（FR-15/FR-22）

**Decision**: `consumer_progress(consumer_name, topic, partition, next_offset)` 在效果事务内推进；Kafka offset 处理成功后在提交；启动/再均衡 position = max(PG next_offset, Kafka committed offset 无效时回退 PG)。lag = 分区 high-watermark − position（Kafka 元数据）与「PG 追赶水位」双口径；追赶时间 = 从恢复时刻到 pending 消费滞后低于阈值的时间（阈值配置）。

**Rationale**: 进度以 PG 为权威载体，Kafka offset 丢失/重置不会导致财务重复（重投递由 inbox 吸收）；双口径可观测满足 FR-15/FR-24。

**Alternatives**: (a) 仅 Kafka offset——管理面重置风险、不可从 PG 重建进度；(b) 仅 PG——再均衡 seek 全依赖 PG 单点（保留为回退路径即可）。

## R9 — 隔离区与人工重放机制（OQ4；PD-4 边界）

**Decision**: `consumer_quarantine`（持久快照 + 失败分类 + 状态 `open|replayed|superseded`）；`event_ops_audit(operation_id UNIQUE, op_kind ∈ {replay, unblock, retention_prune}, operator, scope JSONB, reason, result)`；`events-admin replay --consumer X --scope <event-ids|aggregate|time-range> --reason ...`。授权：沿用本仓库操作员子命令模式（本地阶段授权操作员，PD-4：不要求第二人审批）。重放必经 inbox/版本守卫；**不得**重执行链上付款或创建新提款意图（重放只做「重新投递/重新处理历史事件」）。自动重试在指标与日志上与人工重放分列（`consumer_retry_total` vs `event_replay_ops_total`）。

**Rationale**: 满足 FR-14/PD-4 与 SC-05；`operation_id` 去重沿用 008/011 的运营审计模式；快照保留使重放不依赖原 topic 留存。

**Alternatives**: (a) 直接丢 Kafka 死信 topic 无 PG 载体——审计与重放证据弱；(b) 双人审批——PD-4 明确不要求，且本阶段无既有授权依据。

## R10 — 缓存键、失效与 epoch（OQ3 之一；FR-17/FR-23）

**Decision**: cache-aside；只缓存非权威读模型；键 `txharbor:<epoch>:<family>:<id>`；值含 `(value, source_version, cached_at)`；失效 = 权威转换的 outbox 事件驱动删除受影响 family 键（进程内失效器 + 独立失效循环可复用同一消费者运行时），TTL 兜底（初始 30s，展示类；测量后校准）；Redis 重启/清空 → epoch 轮换，旧 epoch 键不可达；命中时若 `source_version` 落后于已知权威水位（可从 PG 单行读的轻量水位）或新鲜度不可确认 → 标注 `possibly_stale`，财务权威查询一律回源 PG。

**Rationale**: 事件即失效信号，避免「缓存写 + PG 写」双写；epoch 使恢复后不提供已失效陈旧值（FR-17/SC-08）；决策路径不读缓存继承 011 投影纪律。

**Alternatives**: (a) 纯 TTL 无失效——权威变化后窗口内陈旧，违反 FR-17；(b) 写穿缓存（同事务写 Redis）——跨系统原子不可能，制造双写；(c) 双删延迟——复杂且仍非保证。

## R11 — 分布式限流：算法与失效处置（OQ3 之二；PD-1/FR-18）

**Decision**: Redis 限流器（令牌桶/滑动窗口 + Lua 原子脚本），按接口类（新提款创建、一般写、查询、操作员、RPC 预算）配置速率与突发（数值待测，配置边界与测量方法见 [verification.md](verification.md)）；限流不参与授权判定。PD-1 失效处置：新提款创建 → fail-closed 拒绝 + 可重试错误（分类沿用 007 taxonomy，HTTP 429/503 + `Retry-After`）；存量资金流程按原门禁继续；查询回源；RPC 按 R12；**不**引入替代限流。恢复：梯度放开（限速爬坡 + 预热），不瞬间无界。

**Rationale**: 直接实现 PD-1；Redis 仅作限流载体（FR-02），故障时资金写入保守拒绝而非放行。

**Alternatives**: (a) PG 计数限流——权威库承压且热点行争用；Redis 是本 feature 批准的载体；(b) 本地进程限流替代——多实例总量不可界且 PD-1 未批准资金写入替代机制；(c) 失效时全站拒绝——超出 PD-1（存量流程必须继续）。

## R12 — RPC 有界控制降级（PD-1/FR-19）

**Decision**: 出站 RPC 的既有每进程有界控制（连接/请求超时、重试上限、并发上限、错误分类）为**基线控制**，不依赖 Redis；分布式 RPC 预算为叠加治理。Redis 故障时：可维持基线有界的调用类继续（降级为保守并发），仅靠分布式预算才能有界的调用类安全暂停、恢复后续跑；任何情况下不跳过错误分类、链身份校验、完整性检查（FR-19）。

**Rationale**: 诚实区分「有界控制」与「分布式公平」；保留 002–010 已锁定的 RPC 语义（spec 不重定义）。「保守并发继续」不是资金写入的替代限流（那是 PD-1 禁止的范围），而是 RPC 侧既有限界的降级执行。

**Alternatives**: (a) Redis 故障即暂停全部 RPC——可用性损失过大且无必要（多数调用已局部有界）；(b) 放开本地限制维持吞吐——用故障换吞吐，违反「不得借故障绕过门禁/控制」。

## R13 — 容量模型：不变量、公式与测量方法（PD-2/FR-20）

**Decision**: 指标 `outbox_pending_count{class}`、`outbox_pending_oldest_age_seconds{class}`（PG 部分索引查询）。配置 `soft_limit`、`hard_limit`、`reserve`、`retention`、`max_shutdown_window`。测量先行：`max_events_per_admitted_operation`（对每个接纳型操作测其事务内可产生的事件行数上界）、`max_in_flight_operations`（并发模型/配置上界）、`measured_drain_rate`（Kafka 健康时排空速率）。边界公式：`reserve ≥ max_events_per_operation × max_in_flight_ops`；`soft_limit = reserve + drain_rate × target_drain_window`；`hard_limit = soft_limit + reserve`。数值一律待测；配置 fail-closed（`0 < reserve < soft_limit < hard_limit`）。

**行为**：soft 起拒绝可控制的新资金写入（可重试错误）/降级非关键；链上已发生充值不拒绝——若容量不允许安全持久化，从 003/004 可靠进度暂停处理、恢复后补扫（不跳过观察）；在途提款按既有暂停/对账协议收尾。「在途可完成 vs 满」不矛盾证明：[contracts/capacity.md](contracts/capacity.md) §2。

**Rationale**: 满足 PD-2 与 SC-09；把「停新保在途」做成可在接纳前判定的不变量，而非事后补丁。

**Alternatives**: (a) 固定行数阈值写死——spec 禁止编造且无测量依据；(b) 无限积压——违反 FR-20；(c) 到达硬上限时拒绝充值写——直接把事件与状态拆开，违反 FR-07 与 PD-2（链上充值不得拒绝）。

## R14 — 旧数据衔接、迁移与回退

**Decision**: `000015` 纯增量；cutover 标记 `event_system_state.cutover_at`；不伪造历史事件；首见版本即消费者基线；历史状态用只读快照导出初始化（操作员流程，quickstart Q0）。回退 = 受控窗口内完整特性回退：停发布器/消费者 → 盘点 `pending|blocked`（导出/排空）→ down 迁移 → 移除接线；明确「仅停发射而业务继续」不合规（违反 FR-07）。

**Rationale**: 真实反映迁移现实；避免「历史事件回放」制造无法验证的伪审计；回退边界诚实可操作。

**Alternatives**: (a) 全量历史回填——旧状态缺事件身份/版本上下文，伪历史不可信；(b) 不停发射静默降级——违反 FR-07。

## R15 — 观测口径与基准方法学（OQ5/FR-24/FR-25）

**Decision**: 指标清单与采集口径（定义、单位、来源、标签、百分位方法）落 [verification.md](verification.md) §1；对照基准协议落 [adr.md](adr.md)（同负载/同故障、同环境规格、报告字段、表述纪律）。阈值初始建议值标注「初始，测量后校准」；业务阈值（p95/p99/吞吐目标、追赶窗口）不编造。

**Rationale**: FR-24/25/SC-11 要求可产出、可复核的测量方法；把「怎么测」与「结论」分离，避免预判 Kafka 收益（PD-3）。

**Alternatives**: 直接写目标值——无实测依据，spec 明令禁止。

## R16 — 测试分层、触发与预算方法（FR-28）

**Decision**: 分层与触发矩阵落 [verification.md](verification.md) §3–4。要点：Unit 无中间件常跑；Integration-PG 沿用 `-tags integration`；Integration-Redis/Kafka 用新 tag 独立；Contract 层校验信封/版本/兼容；E2E 核心充提；Fault/Perf 独立（不进普通 PR）。触发：变更路径 → 必需检查映射（`internal/events`、`internal/cache`、`internal/ratelimit`、上游集成点、迁移 → 对应层）。预算方法：同 runner 上先测基线（PG-only 路径），预算 = 基线的显式倍数 + CI timeout 硬上限；数值实现批次测量后写入，不编造。

**Rationale**: 直接落实 FR-28 与用户收口；避免普通 PR 启动全部中间件与演练。

**Alternatives**: 全矩阵进 PR——CI 成本与稳定性不可接受（用户已明确约束）。

## R17 — 依赖与本地编排（D10）

**Decision**: 客户端提案 franz-go / go-redis；测试容器模块与 compose 镜像（`redis`、Kafka KRaft 单节点）在实现批次验证可用性后固定 tag；compose 以 profile 增加服务，保持 PG+Anvil 为默认基线（PG-only 对照可运行）。topic/分区/消费组的本地创建由启动时显式创建（不允许 auto-create，避免隐式默认）。

**Rationale**: 纯 Go 客户端避免 CGO；KRaft 免 ZooKeeper；profile 化保证「关 Redis/Kafka」演练与 PG-only 基线是同一编排。

**Alternatives**: (a) librdkafka（confluent-kafka-go）——CGO 与构建复杂度；(b) Kafka + ZooKeeper——本地面更大；(c) Redpanda 本地——API 兼容但生态位不同，保留为 CI 轻量候选，实现批次评估。

## R18 — 安全、脱敏与载荷纪律

**Decision**: 事件载荷与日志只含：事件/业务/链身份、版本、状态/原因、恢复版本、时间、尝试次数、错误分类；禁止：私钥、凭据、API key、原始签名字节、无限制原始请求/响应。日志沿用 `internal/logx` 脱敏；错误输出保留链/高度/哈希/身份/分类（spec Edge Cases 允许项）。`internal/events` import 边界测试禁止 import signer/key provider 与 RPC/dial 包。

**Rationale**: 章程 VIII/XII 与 Security Rules；FR-24 日志要求。

**Alternatives**: 记录原始载荷便于调试——泄露风险不可接受。

---

## NEEDS CLARIFICATION 状态

Technical Context 与 spec OQ1–OQ6 中影响设计的问题已全部由 R1–R18 解决；无未决 `NEEDS CLARIFICATION`。数值类输入（阈值、速率、窗口、预算）按任务规则给测量方法与配置边界，待实现/性能批次实测校准；业务范围调整（Kafka 去留）按 PD-3 另行提交用户裁决。
