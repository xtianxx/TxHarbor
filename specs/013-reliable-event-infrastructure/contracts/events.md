# Contract: Business Event Envelope, Identity, Catalog & Compatibility

**Feature**: 013-reliable-event-infrastructure | **Plan**: [../plan.md](../plan.md) | **Research**: R2–R4 | **Spec**: FR-07/09/10/11/12/16

本契约是「生产者（业务事务）↔ 发布器 ↔ 消费者」之间的稳定接口。载体细节见 [../data-model.md](../data-model.md) Table 1。

## §1 Envelope（事件信封）

每个事件 MUST 携带：

| 字段 | 语义 | 必填条件 |
|---|---|---|
| `event_id` | 传输身份（自然键的确定性 UUIDv5） | 总是 |
| `event_type` | 目录类型 `<domain>.<object>.<fact>` | 总是 |
| `schema_version` | 载荷 schema 版本（整数，从 1 起） | 总是 |
| `identity_kind` | `evm_log` \| `business_object` | 总是 |
| `aggregate_type`/`aggregate_id` | 稳定业务对象身份 | 总是 |
| `aggregate_version` | 发射流内单调版本（从 1 连续） | 总是 |
| `occurred_at` | 事实发生时间（UTC） | 总是 |
| 链身份 | `chain_id`、`block_number`、`block_hash`（日志另含 `tx_hash`、`log_index`） | `evm_log` 与链派生修订必填 |
| `recovery_version` | 006 恢复版本 | 修订事件必填 |
| `revises_event_id` | 被修订事件 | 修订事件必填 |
| `payload` | 最小化载荷（[../data-model.md](../data-model.md) §9） | 总是 |

规范：事件不是执行许可、不是授权、不是链上事实本身（FR-03/FR-05）；「事件已/未送达」不改变 PG 权威状态。

## §2 身份与冲突规则

1. `evm_log` 唯一键 `(chain_id, block_hash, tx_hash, log_index)`；高度 MUST NOT 单独作为身份（003 契约）。
2. `business_object` 唯一键 `(aggregate_type, aggregate_id, aggregate_version)`；业务身份 MUST NOT 依赖投递尝试、时间戳或生产者实例。
3. 同身份同内容（`payload_hash` 相等） = 幂等重复：no-op，不告警。
4. 同身份异内容 = 冲突：MUST 拒绝该事务并告警（`events_identity_conflict_total`），MUST NOT 静默覆盖（FR-09/SC-03）。
5. 版本必须按对象单调且从 1 连续（发射流内）；实现保证：同一事务内 `max+1` + 上游同对象行锁 + UNIQUE 兜底（[../data-model.md](../data-model.md) §3）。

## §3 目录 v1（catalog_version = 1）

| event_type | identity/版本 | 载荷关键字段 | 来源 | 语义要点 |
|---|---|---|---|---|
| `deposit.observation.created` | `evm_log`；对象版本 1（首条） | 观察 id、链身份、来源日志三元组、初始状态 | 004 | 仅表示观察到事实，不表示确认或入账 |
| `deposit.observation.status_changed` | 对象版本 +1 | `from_state`、`to_state`、原因、链身份 | 004/006 | Pending/Orphaned 等转换；Orphaned 表达失效 |
| `deposit.observation.reinstated` | 对象版本 +1 | 复活依据、原观察 id、链身份 | 006 FR-08 | 同旧 `block_hash` 再 canonical 的复活，与新观察区分 |
| `deposit.confirmation.confirmed` | 对象版本 +1 | `policy_version`、确认高度/哈希、链身份 | 005 | 「达到项目确认策略」，不等于上游入账；可因重组失效 |
| `deposit.revision.applied` | 对象版本 +1 | `superseded_identity`（旧事件/旧区块身份）、新 canonical 状态或 Orphaned 处置、原因、`recovery_version` | 006 | 修订既有事实；幂等；非新充值/新确认 |
| `withdrawal.request.received` | 对象版本 +1 | request_id、caller、状态 Accepted | 007 | Accepted 仅表示已接收；不构成执行授权 |
| `withdrawal.execution.state_changed` | 对象版本 +1 | `from_state`/`to_state`、intent_id、attempt 引用、冻结/暂停上下文 | 011 | 执行关键转换事实；不得被解读为发送许可 |
| `withdrawal.execution.revised` | 对象版本 +1 | `superseded_identity`、修订后状态、原因、`recovery_version` | 010/011 | 链上事实修订；不触发新意图/发送 |

扩展规则：新增类型 = 新目录条目 + 新 schema_version；既有类型语义 MUST NOT 原地改变。FR-07 的「至少覆盖」由 `deposit.*`、`withdrawal.request.received`、`withdrawal.execution.*` 满足。

## §4 Schema 版本与兼容规则（FR-12）

1. 每个事件 MUST 带 `schema_version`；消费者按 `(event_type, schema_version)` 解析。
2. 向后兼容变更（新增可选字段、放宽校验）= 保持同版本；消费者 MUST 忽略未知字段。
3. 破坏性变更（删字段、字段语义变化、新增必填）= MUST NOT 原地发布；MUST 提升 `schema_version` 并提供兼容窗口（旧版本继续可消费）与迁移说明。
4. 发布者 MUST NOT 在未提升版本的情况下改变载荷语义。
5. 消费者遇未知/不支持版本 = fail-closed：隔离（`schema_unsupported`）+ 告警，MUST NOT 猜测解析或误用（SC-05）。

## §5 修订语义（FR-11）

1. 修订事件 MUST 至少表达：旧事件/旧区块身份（`revises_event_id` + 载荷 `superseded_identity`）、新 canonical 状态或 Orphaned 处置、修订原因、`recovery_version`。
2. 修订 MUST 幂等（重复/乱序投递收敛）；消费者 MUST 收敛到修订后状态；MUST NOT 继续把 Orphaned 数据作为有效 canonical 事实（SC-07）。
3. 修订 MUST NOT 被解释为新充值、新确认、新付款指令；MUST NOT 触发提款意图、nonce 分配、签名或广播（FR-05/SC-07）。
4. 006 FR-08 同旧 `block_hash` 再次 canonical：`reinstated` 复用原观察复活语义，与 `created`（新来源观察）严格区分，不重复转换。

## §6 链身份校验（FR-10）

消费者的任何链派生处理 MUST 校验 `chain_id` 与本地配置链一致、`block_hash` 与 `block_number` 组合合法；缺失或链身份不匹配 = 拒绝/隔离（`identity_mismatch`）。跨对象全局顺序 MUST NOT 被假定。

## §7 Cutover 基线

事件流从 `event_system_state.cutover_at` 起；历史状态不经事件流重放（不伪造历史，[../data-model.md](../data-model.md) §7）。消费者「首见版本即基线」（[consumer.md](consumer.md) §3）；需要历史状态的消费者用只读快照初始化。修订语义仅适用于 cutover 后发射的事实。
