# Contract: Event Consumer — Idempotency, Versioning, Quarantine, Progress & Replay

**Feature**: 013-reliable-event-infrastructure | **Plan**: [../plan.md](../plan.md) D3 | **Research**: R7–R9 | **Spec**: FR-13/14/15/16/12

## §1 保证范围（准确表述）

- 投递 = **至少一次**：已提交事件不丢失；重复可发生；乱序可发生。
- 处理 = **幂等**：同一事件有效应用次数恰好一次（以消费者 PostgreSQL 效果计）。
- 本系统 **MUST NOT** 宣称跨系统恰好一次（PG ↔ Kafka 无分布式事务）；下游账本正确性不在本项目保证范围内（FR-16），以参考消费者 + 集成证据验证并声明边界。

## §2 应用事务（单一 PG 事务）

1. `INSERT INTO consumer_inbox(consumer_name, event_id, ...) ON CONFLICT (consumer_name, event_id) DO NOTHING`；0 行 = 已应用 → 跳过（幂等，SC-04）。
2. 版本守卫（§3）判定。
3. 业务效果（本项目内：投影/参考消费者/指标状态）。
4. `consumer_progress` 推进到 `offset+1`。
5. 提交；之后异步提交 Kafka offset。
崩溃点语义见 [../data-model.md](../data-model.md) §5 T4：任何崩溃后重投递都收敛到「效果一次」。

## §3 版本守卫与缺口策略

- `consumer_versions.max_version` 仅当 `event.aggregate_version > max_version` 时应用；相等 = 幂等跳过；小于 = 旧版本，跳过并计数（MUST NOT 覆盖新版；SC-04）。
- `> max_version + 1` = 缺口：在配置等待窗口（初始 10s，测量后校准）内可等待更早事件投递；仍缺 → 隔离 `version_gap` + 告警 + 分区继续（MUST NOT 静默跳过、MUST NOT 无限阻塞其他事件/分区；FR-10/FR-14）。
- **首见基线**：对象无 `consumer_versions` 记录时，首见事件的版本即基线（兼容 cutover 后从 1 起的事件流；[events.md](events.md) §7）。
- 链身份校验失败（链不匹配/缺字段）= 隔离 `identity_mismatch`（[events.md](events.md) §6）。

## §4 重试、隔离与毒事件

- 可重试类（瞬时依赖/锁竞争）：有界退避（base 500ms、cap 30s、上限 8 次——初始值，测量后校准），每次尝试都重走 §2（inbox 已插入则不重复效果）。
- 不可重试类或超上限：入 `consumer_quarantine`（快照 + `failure_class` + 原因 + 尝试数），告警；**不静默丢弃**、**不无限阻塞**其他事件或分区（SC-05）。
- 隔离后的分区进度继续推进（事件已持久快照，可在修复后重放）。
- `consumer_inbox`/`consumer_versions`/`consumer_progress` 的更新 MUST 在同一事务；MUST NOT 仅依赖内存（章程 II）。

## §5 进度、重启、再均衡与 lag

- `consumer_progress(consumer_name, topic, partition, next_offset)` 为权威续传点；重启/再均衡 position = max(PG `next_offset`, Kafka committed offset 回退)，再均衡后逐分区顺序消费。
- lag 双口径：Kafka 分区滞后 + PG 追赶水位；追赶时间可观测（FR-15/FR-22；口径 [../verification.md](../verification.md) §1）。
- offset 提交失败/丢失重放：inbox 去重收敛，0 重复财务效果（SC-06）。

## §6 人工重放（PD-4）与自动重试的区分

- 载体：`events-admin replay --consumer <name> --scope <event-ids|aggregate|time-range|offset-range> --reason <text> --operator <id>`；写 `event_ops_audit(op_kind='replay', scope, reason, result)`，`operation_id` 去重（CLI 重试幂等）。
- 边界：重放 = 重新投递/重新处理历史事件；MUST 受持久幂等、版本守卫、既有安全门禁约束；MUST NOT 绕过；**MUST NOT 重新执行链上付款或创建新提款意图**（PD-4/FR-14）。
- 授权：本地阶段授权操作员执行，不要求第二人审批（PD-4）；不引入新审批关卡。
- 区分：自动重试记录于 `consumer_retry_total`；人工重放/解阻记录于 `event_replay_ops_total` 与审计表——指标与审计口径分离（FR-14）。
- 隔离解阻（`unblock`，发布器 `blocked`）同走审计。

## §7 未知 schema 版本

未知/不支持 `schema_version` → fail-closed：隔离 `schema_unsupported` + 告警；MUST NOT 猜测解析或部分应用（FR-12/SC-05）。

## §8 参考消费者与验证边界（FR-16）

- `event-consumer`（`internal/events/refconsumer`）为验收证据消费者：用 §2/§3 机制演示「同一事件有效应用次数 = 1、模拟账本效果不重复」。
- 明确声明：参考消费者 **非生产账本、非权威**、不持用户余额；其证据只覆盖「本项目事件身份/版本/投递语义 + 消费者幂等契约」，MUST NOT 被表述为对外部真实账本的正确性保证（SC-12）。
- 参考消费者使用独立 `consumer_name`，与任何下游消费者隔离；其状态可随时重建。

## §9 消费者 MUST NOT 做的事

- MUST NOT 把事件送达/处理当作授权、暂停解除、对账结论或发送许可（FR-05）。
- MUST NOT 因重复投递创建新提款意图、分配 nonce、签名或广播。
- MUST NOT 将进度/去重存入 Redis 单点（FR-02/FR-15）。
