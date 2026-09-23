# Contract: Outbox Publisher & Delivery Semantics

**Feature**: 013-reliable-event-infrastructure | **Plan**: [../plan.md](../plan.md) D2 | **Research**: R6 | **Spec**: FR-08/FR-21 | **Data**: [../data-model.md](../data-model.md) Table 1

## §0 状态与边界

`outbox_events.publish_state ∈ {pending, published, blocked}`。发布器只处理 `pending`；`published` 表示消息已按 Kafka topic 策略被 broker 确认；`blocked` 表示永久类失败被显式阻塞（告警 + 可审计解阻），**绝不**表示丢弃。多实例发布器允许重复发布；消费者负责幂等（[consumer.md](consumer.md)）。发布器 MUST NOT 在持有事务时做网络发布；MUST NOT 发布未提交行（领取仅发生在已提交的 `pending` 行上）。

## §1 领取协议（多实例安全）

1. 单事务：`SELECT id FROM outbox_events WHERE publish_state='pending' AND next_attempt_at <= now() ORDER BY id LIMIT $batch FOR UPDATE SKIP LOCKED` → `UPDATE ... SET claim_owner=$instance, claim_expires_at=now()+lease, attempt_count=attempt_count+1 WHERE id=ANY(...)` → 提交。
2. 事务外：按 `aggregate_type:aggregate_id` 分区键 `ProduceSync`（批内有界并发）。
3. ack 后独立事务：`UPDATE ... SET publish_state='published', published_at=now(), claim_owner=NULL, claim_expires_at=NULL WHERE id=ANY($ok) AND claim_owner=$instance`；未确认项按 §4 释放/退避。
4. 租约（`lease`）为技术初始值，测量后校准；租约到期即视为实例死亡，可被重领（重复发布可接受）。
5. 崩溃语义：提交前崩溃无副作用；领取后崩溃 → 重领重发；ack 后标记前崩溃 → 重发；标记后崩溃 → 不重发（重复已由 at-least-once 覆盖）。**不宣称**跨 PG/Kafka 恰好一次。

## §2 顺序边界

- 分区键 = `aggregate_type:aggregate_id`：同对象事件期望落同一分区。
- 分区内顺序在单 producer 会话内尽力保持（幂等 producer + `max.in.flight ≤ 5`）；重领取、重试、多实例、扩容分区会引入重复与乱序可能。
- **正确性边界在消费者**：对象级版本守卫（[consumer.md](consumer.md) §3）保证旧版本不覆盖新版本、缺口不静默跳过。发布器 MUST NOT 宣称全序或恰好一次。

## §3 投递确认语义

- Producer 配置：`acks=all`、`enable.idempotence=true`、`max.in.flight.requests.per.connection ≤ 5`、`delivery.timeout.ms` 有界、`request.timeout.ms` 有界、`retries` 由客户端在 delivery timeout 内自动管理。
- 确认 = broker 接受（按 topic 策略持久化）；**不**代表消费者已处理。
- Topic：`txharbor.events.v1`；分区初始 6（初始值，测量后校准）；关 auto-create（显式创建，避免隐式默认）。
- 元数据/消息键包含 `event_id`、`event_type`、`schema_version`（消费者路由与追踪用，不改变信封契约）。

## §4 失败分类与有界尝试

| 类别 | 例 | 处理 |
|---|---|---|
| 瞬时（可重试） | broker 不可达、超时、限流、leader 切换 | 释放领取；`next_attempt_at = now() + backoff(attempt_count)`（base 1s、cap 60s、±20% 抖动——初始值，测量后校准）；保持 `pending` |
| 永久（不可重试） | 载荷序列化失败、契约校验失败、无效 topic | `publish_state='blocked'` + `last_error_class` + 告警；操作员修复后 `events-admin unblock`（审计） |
| 背压 | Kafka 不可用长时间 | 保持 `pending`（容量保护介入，[capacity.md](capacity.md)）；不丢弃、不覆盖 |

**边界论证**：尝试次数不设「丢弃上限」（丢弃违反 FR-08/I/VI）；有界性体现在单次尝试时长、批量、并发与退避速率上；`blocked` 使永久失败可见、可审计、可人工恢复（章程 IX 的显式论证见 [../plan.md](../plan.md) Complexity Tracking）。

## §5 恢复与追赶

1. Kafka 恢复后，发布器从持久 `pending` 排空；排空速率由批量/并发/退避配置有界（不无界冲击 broker 或 PG）。
2. 追赶期间再次故障 → 未确认项回到 `pending`/退避，已 ack 项保持 `published`；0 丢失。
3. 指标：`outbox_pending_count`、`outbox_pending_oldest_age_seconds`、`outbox_publish_failures_total{class}`、`outbox_published_total`（口径见 [../verification.md](../verification.md) §1）。

## §6 可观测与审计

- 发布失败 MUST 可观测并可告警（FR-21）；最老待发等待超软边界触发容量行为（[capacity.md](capacity.md)）。
- 解阻/重放经 `event_ops_audit`（`op_kind='unblock'` 或 `replay`）记录操作者、范围、理由、结果（PD-4）。
- 裁剪（`retention_prune`）仅删除 `published` 行且记录审计水位；`pending|blocked` MUST NOT 被删除或覆盖。
