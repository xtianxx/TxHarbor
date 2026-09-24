# Phase 1 Data Model: 013 Reliable Event Infrastructure

**Feature**: 013-reliable-event-infrastructure | **Date**: 2026-09-24 | **Plan**: [plan.md](plan.md) | **Research**: [research.md](research.md)

范围：本文件定义 013 新增的 PostgreSQL 载体（唯一迁移 `migrations/000015_event_infrastructure.sql`，纯增量 DDL）、事件身份/版本派生规则、上游集成点、事务与崩溃目录、容量不变量、上线/回退程序。Redis/Kafka 侧无持久 schema（非权威）；其契约见 [contracts/](contracts/)。

约定：时间列 `TIMESTAMPTZ`（UTC）；版本/计数/offset 一律 `BIGINT`/`INTEGER`（无浮点，章程 I）；哈希小写十六进制；`event_id`/标识符 `TEXT`；载荷 `JSONB`。所有约束名以 `outbox_`/`consumer_`/`event_` 前缀命名，便于 23505/23514 精确分类（沿用上游 pattern）。

---

## 1. 表清单总览

| # | 表 | 目的 | 对应 FR |
|---|---|---|---|
| 1 | `outbox_events` | 同事务事件记录 + 发布状态 + 容量观测 | FR-07/08/09/10/20/21 |
| 2 | `consumer_progress` | 消费者持久进度（offset 高水位） | FR-15/22 |
| 3 | `consumer_inbox` | 事件级持久去重（恰好一次效果证据） | FR-13 |
| 4 | `consumer_versions` | 对象级版本守卫与缺口检测 | FR-10/13 |
| 5 | `consumer_quarantine` | 毒事件/未知版本持久隔离 | FR-12/14 |
| 6 | `event_ops_audit` | 人工重放/解阻/裁剪的操作审计（PD-4） | FR-14 |
| 7 | `event_system_state` | cutover 标记与目录版本（单行） | FR-07（上线衔接） |

## 2. 表定义

### Table 1 — `outbox_events`

| 列 | 类型 | 约束/说明 |
|---|---|---|
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | PK；发布领取的排序键（FIFO 近似） |
| `event_id` | `UUID NOT NULL` | 传输身份；自然键的 UUIDv5 确定性派生；UNIQUE |
| `identity_kind` | `TEXT NOT NULL` | CHECK `IN ('evm_log','business_object')` |
| `event_type` | `TEXT NOT NULL` | 目录类型（[contracts/events.md](contracts/events.md) §3） |
| `schema_version` | `INTEGER NOT NULL` | CHECK `> 0` |
| `aggregate_type` | `TEXT NOT NULL` | 业务对象类（如 `deposit_observation`、`withdrawal_request`、`withdrawal_intent`） |
| `aggregate_id` | `TEXT NOT NULL` | 稳定业务身份（不依赖投递尝试） |
| `aggregate_version` | `BIGINT NOT NULL` | 发射流内连续版本，CHECK `> 0`（§3） |
| `payload` | `JSONB NOT NULL` | 最小化载荷（§9） |
| `payload_hash` | `TEXT NOT NULL` | payload 规范字节的 sha256（64 hex）；冲突检测（§3） |
| `occurred_at` | `TIMESTAMPTZ NOT NULL` | 业务事实发生时间 |
| `chain_id` | `BIGINT NULL` | 链派生事件必填 |
| `block_number` | `BIGINT NULL` | 链派生事件必填（身份不单独用高度） |
| `block_hash` | `TEXT NULL` | `evm_log` 必填 |
| `tx_hash` | `TEXT NULL` | `evm_log` 必填 |
| `log_index` | `INTEGER NULL` | `evm_log` 必填 |
| `recovery_version` | `BIGINT NULL` | 修订事件必填（006 恢复版本） |
| `revises_event_id` | `UUID NULL` | 修订指向的旧事件 |
| `source_kind` | `TEXT NULL` | 对账审计用：来源表/域（如 `deposit_observer`） |
| `source_id` | `TEXT NULL` | 来源事实稳定 id |
| `source_version` | `BIGINT NULL` | 来源版本（若存在），仅审计/追溯 |
| `publish_state` | `TEXT NOT NULL DEFAULT 'pending'` | CHECK `IN ('pending','published','blocked')` |
| `attempt_count` | `INTEGER NOT NULL DEFAULT 0` | CHECK `>= 0` |
| `next_attempt_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | 退避调度 |
| `claim_owner` | `TEXT NULL` | 领取实例 id（临时字段，非状态） |
| `claim_expires_at` | `TIMESTAMPTZ NULL` | 领取租约到期 |
| `last_error_class` | `TEXT NULL` | 最近失败分类（瞬时/永久/契约） |
| `published_at` | `TIMESTAMPTZ NULL` | ack 后标记 |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

约束与索引：

- `outbox_events_log_identity_uniq`: UNIQUE `(chain_id, block_hash, tx_hash, log_index)` WHERE `identity_kind='evm_log'`（FR-09；高度不单独作身份）。
- `outbox_events_object_identity_uniq`: UNIQUE `(aggregate_type, aggregate_id, aggregate_version)` WHERE `identity_kind='business_object'`。
- CHECK `outbox_events_log_identity_shape`: `identity_kind='evm_log'` ⇒ `chain_id/block_number/block_hash/tx_hash/log_index` 全非空。
- CHECK `outbox_events_state_consistency`: `published` ⇒ `published_at IS NOT NULL`；`pending|blocked` ⇒ `published_at IS NULL`；`blocked` ⇒ `last_error_class IS NOT NULL`。
- CHECK `outbox_events_revision_shape`: `revises_event_id IS NOT NULL` ⇒ `recovery_version IS NOT NULL`（修订必须携带恢复版本，FR-11）。
- `outbox_events_pending_queue_idx`: `(next_attempt_at, id)` WHERE `publish_state='pending'`（发布器领取）。
- `outbox_events_pending_capacity_idx`: `(id)` WHERE `publish_state='pending'`（容量计数/最老等待）。
- `outbox_events_published_retention_idx`: `(published_at)` WHERE `publish_state='published'`（保留期裁剪）。
- `outbox_events_source_audit_idx`: `(source_kind, source_id, source_version)`（对账审计）。

写入协议（`Append`）：`INSERT ... ON CONFLICT (<上表对应自然键>) DO UPDATE SET attempt_count = outbox_events.attempt_count WHERE outbox_events.payload_hash = EXCLUDED.payload_hash RETURNING id`；0 行 = 同身份异内容 → 事务拒绝 + `events_identity_conflict_total` 告警。同身份同内容 = 幂等 no-op（返回既有行，不重复告警）。

### Table 2 — `consumer_progress`

| 列 | 类型 | 约束 |
|---|---|---|
| `consumer_name` | `TEXT NOT NULL` | PK 组成 |
| `topic` | `TEXT NOT NULL` | PK 组成 |
| `partition` | `INTEGER NOT NULL` | PK 组成 |
| `next_offset` | `BIGINT NOT NULL` | CHECK `>= 0`；下一待处理 offset（高水位） |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

PK `(consumer_name, topic, partition)`。用途：效果事务内推进；重启/再均衡的权威续传点（[contracts/consumer.md](contracts/consumer.md) §5）。

### Table 3 — `consumer_inbox`

| 列 | 类型 | 约束 |
|---|---|---|
| `consumer_name` | `TEXT NOT NULL` | PK 组成 |
| `event_id` | `UUID NOT NULL` | PK 组成；去重键 |
| `aggregate_type` / `aggregate_id` | `TEXT NOT NULL` | 证据查询 |
| `aggregate_version` | `BIGINT NOT NULL` | 证据查询 |
| `topic` / `partition` / `offset` | `TEXT`/`INTEGER`/`BIGINT NOT NULL` | 投递来源（审计） |
| `applied_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

PK `(consumer_name, event_id)`；索引 `(consumer_name, aggregate_type, aggregate_id)`。语义：插入成功 = 本事件首次效果应用；23505 = 已应用，跳过（FR-13）。

### Table 4 — `consumer_versions`

| 列 | 类型 | 约束 |
|---|---|---|
| `consumer_name` / `aggregate_type` / `aggregate_id` | `TEXT NOT NULL` | PK 组成 |
| `max_version` | `BIGINT NOT NULL` | CHECK `> 0` |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

PK `(consumer_name, aggregate_type, aggregate_id)`。语义：仅 `event.aggregate_version > max_version` 应用；`> max_version+1` = 缺口（§5 缺口策略）。

### Table 5 — `consumer_quarantine`

| 列 | 类型 | 约束 |
|---|---|---|
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | PK |
| `consumer_name` | `TEXT NOT NULL` | |
| `event_id` | `UUID NOT NULL` | |
| `event_snapshot` | `JSONB NOT NULL` | 入隔离时的完整事件快照（防源裁剪） |
| `failure_class` | `TEXT NOT NULL` | `retry_exhausted` / `non_retryable` / `version_gap` / `schema_unsupported` / `identity_mismatch` |
| `reason` | `TEXT NOT NULL` | 人工可读 |
| `attempt_count` | `INTEGER NOT NULL` | |
| `source_topic` / `source_partition` / `source_offset` | | 定位 |
| `first_seen_at` / `last_seen_at` | `TIMESTAMPTZ NOT NULL` | |
| `status` | `TEXT NOT NULL DEFAULT 'open'` | CHECK `IN ('open','replayed','superseded')` |
| `replayed_at` | `TIMESTAMPTZ NULL` | |
| `replay_operation_id` | `TEXT NULL` | 指向 `event_ops_audit.operation_id` |

部分 UNIQUE `(consumer_name, event_id) WHERE status='open'`（同一事件只有一个开放隔离项）。隔离不删除事件、不阻塞分区：跳过并继续（FR-14/SC-05）。

### Table 6 — `event_ops_audit`

| 列 | 类型 | 约束 |
|---|---|---|
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | PK |
| `operation_id` | `TEXT NOT NULL UNIQUE` | 操作去重（CLI 重试/崩溃收敛，沿用 008 R7/011 模式） |
| `op_kind` | `TEXT NOT NULL` | CHECK `IN ('replay','unblock','retention_prune')` |
| `operator` | `TEXT NOT NULL` | 授权操作员标识（PD-4） |
| `scope` | `JSONB NOT NULL` | 所选事件范围（consumer_name + 事件 id 集/聚合/时间窗/offset 范围） |
| `reason` | `TEXT NOT NULL` | |
| `result` | `TEXT NOT NULL` | 结果摘要（如 `replayed=3, skipped_duplicate=2`） |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

PD-4 审计字段为操作者、范围、理由、结果；不要求第二人审批；操作不产生新的付款动作。

### Table 7 — `event_system_state`

| 列 | 类型 | 约束 |
|---|---|---|
| `id` | `SMALLINT PRIMARY KEY` | CHECK `id = 1`（单行） |
| `cutover_at` | `TIMESTAMPTZ NOT NULL` | 013 上线割点（§7） |
| `catalog_version` | `INTEGER NOT NULL` | 事件目录版本（初始 1） |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | |

## 3. 身份与版本派生规则

1. **`evm_log` 派生**（004/005/006 链侧事实）：`chain_id`、`block_hash`、`tx_hash`、`log_index` 来自来源日志；`aggregate` 指向其业务对象（观察/充值）；`event_id = uuidv5(namespace, "evm_log|chain|block_hash|tx_hash|log_index")`。
2. **`business_object` 派生**：`aggregate_type:aggregate_id` 为稳定业务身份（如 `deposit_observation:<obs_id>`、`withdrawal_request:<request_id>`、`withdrawal_intent:<intent_id>`）；`aggregate_version` 为该对象**发射流内**从 1 连续递增（= `coalesce(max(aggregate_version),0)+1`，在同一事务内计算）；`event_id = uuidv5(namespace, "business_object|type|id|version")`。
3. **并发前提**：调用方 MUST 已在同一事务中持有该 aggregate 的源行锁（上游状态机本就对同对象转换串行化）；版本竞争由 `outbox_events_object_identity_uniq` 兜底，冲突按 §10 分类处理（同内容 no-op / 异内容拒绝 + 告警），不得重试掩盖。
4. **修订事件**：新事件（新版本）携带 `revises_event_id` + `recovery_version` + 旧链身份字段（载荷内 `superseded_identity`）；不修改旧行（append-only）。
5. **载荷哈希**：`payload_hash` 按规范化 JSON（键排序、无空白差异）计算；重放/重试使用同一载荷字节，禁止重新序列化漂移。

## 4. 上游集成点（`Append` 调用位）

集成以「同一事务内调用」为硬性要求；实现批次在各写入函数的事务内接入。文件级清单（只读上游，不改其语义）：

| 域 | 集成点（现仓库文件） | 事件 |
|---|---|---|
| 004 观察生成 | `internal/indexer/depositcommit.go`（`commitDepositUnit` 观察插入事务） | `deposit.observation.created` |
| 004/006 观察状态转换 | `internal/indexer/reorgcommit.go`（恢复提交事务）、`internal/indexer/depositscanner.go` 相关转换提交 | `deposit.observation.status_changed` / `.reinstated` |
| 005 确认转换 | `internal/indexer/confirmcommit.go`（确认提交事务） | `deposit.confirmation.confirmed` |
| 006 修订 | `internal/indexer/reorgcommit.go`（重组失效/修订提交事务） | `deposit.revision.applied` |
| 007 请求接收 | `internal/withdrawal/intake.go`（`SubmitWithdrawal` 事务） | `withdrawal.request.received` |
| 011 执行转换 | `internal/execution/intent.go`（`TransitionIntent` 所在事务）、`internal/execution/revision.go`（`Apply` 修订事务）、`internal/execution/advance.go`（`converge` 事务） | `withdrawal.execution.state_changed` / `.revised` |

边界：`internal/events` MUST NOT import 上游 writer 包或 RPC/dial 包；上游集成经 `Append`（events → 上游方向的反向 import 不允许）。010 尝试级事实不在目录 v1 最小集内（链路以 011 `state_changed` + attempt 引用表达）；目录可后续扩展。

对账审计（`events-audit`，可在发布器循环内周期执行）：按 `source_kind/source_id/source_version` 与来源表最近水位比对，缺口 → 指标 + 告警（检测兜底；修复经 `events-admin` 审计路径，不静默补写）。

## 5. 事务与崩溃目录

| ID | 事务 | 内容 | 崩溃点与恢复 |
|---|---|---|---|
| T1 | 业务转换 + Outbox | 上游状态转换 + `Append`（含版本计算/冲突检测） | 崩溃在提交前 → 业务与事件都无；提交后 → 都有（原子）。不存在中间态 |
| T2 | 发布领取 | `SELECT ... FOR UPDATE SKIP LOCKED` → 置 `claim_owner/claim_expires_at` → 提交 | 提交前崩溃无副作用；提交后崩溃 → 租约到期后可重领 → 可能重复发布（允许） |
| T3 | 发布确认 | Kafka ack 后 `UPDATE ... WHERE id AND claim_owner` 置 `published` | ack 前崩溃 → 重发；ack 后标记前崩溃 → 重发；标记后崩溃 → 无重复（重复已由 at-least-once 覆盖） |
| T4 | 消费应用 | inbox 插入 + 版本守卫 + 效果 + `consumer_progress` 推进（单事务） | 提交前崩溃 → Kafka offset 未提交 → 重投递 → inbox 去重；提交后 offset 提交前崩溃 → 重投递 → inbox 跳过（不重复效果） |
| T5 | 隔离 | 快照入 `consumer_quarantine`（+ 指标） | 幂等：部分唯一约束防重复开放项 |
| T6 | 人工重放 | `event_ops_audit` 写入 + 重投递/重处理（经 T4） | `operation_id` 去重防 CLI 重试双执行 |
| T7 | 裁剪 | `published` 行按保留期删除（`retention_prune` 审计） | 永不触碰 `pending|blocked`；审计记录水位 |

## 6. 容量模型与不变量（PD-2）

**指标**：`outbox_pending_count`、`outbox_pending_oldest_age_seconds`（均按 `event_type` 类聚合；来源：`outbox_events_pending_capacity_idx` 查询，Redis 不参与）。

**配置与公式**（数值待测；测量方法与配置边界见 [contracts/capacity.md](contracts/capacity.md)）：

- `E_max` = 单次已接纳操作在 T1 中可产生的最大事件行数（逐操作类实测）。
- `F_max` = 最大在途操作数（并发模型/配置上界，实测）。
- `D` = Kafka 健康时 outbox 排空速率（实测，事件/秒）。
- `W` = 目标排空窗口（配置项；业务可裁决）。
- `reserve = E_max × F_max × safety_factor`（`safety_factor > 1` 实测确定）。
- `soft_limit = reserve + D × W`；`hard_limit = soft_limit + reserve`。
- fail-closed 校验：`0 < reserve < soft_limit < hard_limit`，否则拒绝启动。

**不变量（I-CAP）**：容量保护在**接纳新可控制工作之前**判定（软边界拒新、非关键降级）；每个已接纳单元的事件行在 T1 中与业务同事务，因此接纳后必然可写；`reserve` 覆盖所有在途单元的最大事件写放大，故 `pending ≤ hard_limit` 时在途仍可完成。「Outbox 已满」只可能由**不可拒绝**的链上事件（暂停处理、恢复后补扫，不拒绝事实）与已接纳存量造成，二者与「在途可完成」不矛盾。测试用断言：容量门禁触发后，已接纳操作 100% 提交成功且事件行齐备；`pending` 永不出现「业务提交无事件行」的缺口。

**行为表**：

| pending 区间 | 可控制新资金写入 | 链上观察/确认/修订 | 在途提款 | 非关键功能 |
|---|---|---|---|---|
| `< soft_limit` | 正常 | 正常 | 正常 | 正常 |
| `[soft, hard)` | 拒绝（可重试错误，[contracts/redis.md](contracts/redis.md) 同款错误通道） | 正常（保留容量） | 正常 | 降级 |
| `≥ hard`（若发生） | 拒绝 | 无法安全持久化 → 从 003/004 可靠进度暂停、恢复后补扫 | 按既有暂停/对账协议收尾，不新建意图 | 暂禁 |

## 7. 上线衔接（cutover）、迁移与回退

- 迁移：`000015_event_infrastructure.sql`（纯增量；7 表 + 约束/索引 + 单行 `event_system_state` 种子）。goose 已应用历史不重写；编号在 `000014` 之后。
- cutover：应用迁移后，`event_system_state.cutover_at = now()`；代码上线后所有目录转换开始发射。**不伪造历史事件**。
- 消费者初始化：`consumer_versions` 无记录时「首见版本即基线」（消费者实现规则，[contracts/consumer.md](contracts/consumer.md) §3）；下游需历史状态时用只读快照导出（quickstart Q0）先初始化。
- 回退：受控窗口内完整特性回退——停 `event-publisher`/消费者/参考消费者 → 盘点并导出 `pending|blocked`（或先排空）→ 执行 down（drop 新表；`consumer_*` 数据随之丢失，属可接受的重建成本）→ 移除接线。**仅停发射而业务继续是违规操作**（违反 FR-07），不得作为回退方案。
- 兼容：本迁移不影响任何既有表/列/查询；`000001`–`000014` 不动。

## 8. 发布/隔离状态机

```text
outbox_events.publish_state:
  pending ──(claim + ack)──────────────▶ published ──(retention prune)──▶ [removed]
  pending ──(permanent class)──────────▶ blocked ──(events-admin unblock,
                                                   audited)─────────────▶ pending

consumer_quarantine.status:
  open ──(events-admin replay, audited)─▶ replayed
  open ──(superseded by newer decision)─▶ superseded
```

非法转换（如 `published → pending`、对 `blocked` 静默置 `published`）一律拒绝（CHECK + 条件更新 0 行即拒绝）。

## 9. 载荷与脱敏纪律

载荷允许：身份（事件/业务/链）、版本（aggregate/schema/recovery）、状态与原因枚举、金额（整数/十进制字符串仅作显示，来源为整数）、时间、attempt 引用、`superseded_identity`。
载荷禁止：私钥、助记词、API key/凭据、原始签名字节、完整 calldata 之外的秘密材料、无限制原始请求/响应。实现批次加 SQL/日志扫描测试（011 同款）。

## 10. 错误分类映射

| PG 错误 | 分类 | 处理 |
|---|---|---|
| 23505 `outbox_events_object_identity_uniq`（同 payload_hash） | 幂等重复 | no-op（返回既有行） |
| 23505 同上（异 payload_hash） | 身份冲突 | 拒绝事务 + 告警 + 人工调查 |
| 23505 `consumer_inbox_pkey` | 已应用 | 跳过 |
| 23505 `consumer_quarantine_open_uniq` | 已隔离 | 跳过 |
| 23505 `event_ops_audit_operation_id_key` | 操作重复 | 返回既有审计结果（CLI 收敛） |
| 23514 CHECK 违反 | 契约/编程错误 | 拒绝 + 告警（不重试） |
