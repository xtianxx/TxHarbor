# Quickstart & Validation Guide: 013 Reliable Event Infrastructure

**Feature**: 013-reliable-event-infrastructure | **Date**: 2026-09-24 | **Plan**: [plan.md](plan.md) | **Verification**: [verification.md](verification.md)

**性质声明（重要）**：本文件是**设计阶段的验证/演练剧本**。013 的实现（Redis/Kafka 接线、发布器、消费者、缓存/限流、迁移 000015）尚不存在——本轮不写代码、不执行演练；下文命令中标注「实现批次」的在 tasks/implement 后可用，标注「现状可用」的是当前仓库已有命令。所有场景的通过判定与证据口径以 [verification.md](verification.md) 为准；阈值一律待测，本文件不编造数值。

---

## 1. 环境与前置

| 项 | 现状 | 实现批次（013） |
|---|---|---|
| PostgreSQL + Anvil | `docker compose up -d`（`compose.yaml`，现状可用） | 不变 |
| Redis / Kafka | 未编排 | compose 增加 `redis`、Kafka（KRaft 单节点），以 profile 提供（如 `--profile events`），默认基线仍为 PG+Anvil（PG-only 对照可用） |
| 迁移 | goose 自动应用（`cmd/txharbor migrate`，现状可用） | 追加 `000015_event_infrastructure.sql` |
| 子命令 | `serve` / `migrate` / `signer-serve` / `withdrawal-worker` / `withdrawal-exec`（现状可用） | 增加 `event-publisher` / `events-admin` / `event-consumer` |
| 测试命令 | `make test` / `make test-race` / `make test-integration`（现状可用） | 增加 `make test-integration-redis` / `test-integration-kafka` / `test-e2e` / `test-fault` / `test-perf`（触发与预算见 [verification.md](verification.md) §3–4） |
| 事件目录 | 不存在 | `event_system_state(catalog_version=1)`；目录见 [contracts/events.md](contracts/events.md) §3 |

PG-only 基线运行（对照与故障演练的前提）：只启 PG+Anvil，不启 Redis/Kafka；013 接线以配置开关保持「PG-only 路径可运行」（发布器/消费者不启动，业务继续按 PG 权威处理，事件在 outbox 累积或开关关闭时不发射——发射关闭仅允许用于对照实验与回退演练，不作为生产运行形态）。配置开关与默认值在实现批次定义（fail-closed）。

## 2. 场景索引

| ID | 场景 | 层次（FR-28） | 覆盖 |
|---|---|---|---|
| Q0 | Cutover 基线与快照导出 | 手工/运维 | FR-07、data-model §7 |
| Q1 | 同事务原子性探针 | Integration-PG | FR-07、SC-03 |
| Q2 | 发布器崩溃/重复/多实例 | Integration-Kafka + Fault | FR-08/21、SC-03 |
| Q3 | 消费幂等与版本守卫（重复/乱序/缺口） | Integration-PG + Contract | FR-10/13、SC-04 |
| Q4 | 重试/隔离/人工重放（PD-4） | Integration-PG + Fault | FR-12/14、SC-05 |
| Q5 | 缓存陈旧与击穿保护 | Integration-Redis | FR-17、SC-08 |
| Q6 | 限流失效处置（PD-1） | Integration-Redis + E2E | FR-18、SC-08 |
| Q7 | 容量保护「停新保在途」 | Integration-PG + Fault | FR-20、SC-09 |
| Q8 | 五态故障矩阵演练（双故障：关 Redis+Kafka，保 PG+链） | 独立 Fault Injection | FR-04/05/06/26、SC-01/02/12 |
| Q9 | 恢复追赶与再故障 | 独立 Fault Injection | FR-21/22/23、SC-09/10 |
| Q10 | PG-only vs 全栈对照基准 | 独立 Performance | FR-25、SC-11 |
| Q11 | 重组修订与 block_hash 复活 | Integration-PG + Contract | FR-11、SC-07 |

## 3. 关键场景步骤（设计）

### Q0 — Cutover 基线与快照（[data-model.md](data-model.md) §7）

1. 迁移后确认 `event_system_state.cutover_at` 已写入、`catalog_version=1`。
2. 只读快照导出（实现批次提供脚本/命令，如 `events-admin bootstrap-export`）供下游初始化；验证导出过程**不写业务表、不发射伪造历史事件**。
3. 通过判定：导出前后业务表计数不变；事件表中不存在 cutover 之前 `occurred_at` 的「补造」行。

### Q1 — 原子性探针

1. 对每个集成点（[data-model.md](data-model.md) §4）构造「事务提交」与「事务回滚」两条路径。
2. 通过判定：提交路径业务行与 `outbox_events` 行同时存在且 `aggregate_version` 连续；回滚路径两者都不存在（0 残留）。断言口径见 [verification.md](verification.md) §2 V-ATOMICITY。

### Q2 — 发布器崩溃/重复

1. 停机 Kafka；产生事件（业务继续）。
2. 恢复 Kafka；在发布器 `ProduceSync` 前后注入 kill/重启；重复启动第二个发布器实例。
3. 通过判定：0 事件丢失；重复消息可被消费者吸收（Q3）；`published` 标记最终一致；多实例领取不重复处理同一行（SKIP LOCKED 断言：同一行同一时刻只有一个 owner）。

### Q3 — 消费幂等/乱序/缺口

1. 同一事件重复投递（含重启、rebalance）；旧版本晚到；构造版本缺口。
2. 通过判定：`consumer_inbox` 中 `(consumer_name,event_id)` 唯一；`consumer_versions.max_version` 单调；应用效果计数 = 1；旧版本 0 覆盖；缺口 → 等待后有界隔离且告警，不静默跳过、不阻塞分区。

### Q4 — 重试/隔离/重放

1. 注入可重试失败（临时错误）与不可重试失败；耗尽重试后检查隔离；修复后 `events-admin replay`。
2. 通过判定：隔离 100% 持久可审计；重放后效果恰好一次；`event_ops_audit` 记录操作者/范围/理由/结果；无第二人审批要求；重放未触发任何提款意图/nonce/签名/广播（断言：相关表计数不变）。

### Q5 — 缓存陈旧与击穿

1. 建立缓存条目 → 改变权威状态 → 立即查询受影响键范围。
2. 关闭/清空 Redis → 查询；恢复 Redis → 再查询。
3. 通过判定：0 次返回陈旧财务权威；缓存不可用时直读 PG 且回源并发有界（信号量/单飞生效）；恢复后 0 次提供已失效值（epoch/失效生效）；新鲜度标注正确。

### Q6 — 限流失效（PD-1）

1. 停止 Redis（或使限流脚本失败）。
2. 分别调用：新提款创建、充值/确认/恢复路径、已接受提款执行、查询。
3. 通过判定：新提款创建 100% 拒绝且错误明确可重试（429/503 + `Retry-After`）；0 次无限制放行；存量资金流程按原门禁继续（无门禁绕过）；查询回源可用；恢复后平滑放开（无瞬间无界）。

### Q7 — 容量保护

1. 停机 Kafka，持续产生事件至 `pending ≥ soft_limit`（阈值由配置，先按公式注入小值便于测试）；继续可控写入与链上充值。
2. 通过判定：可控新资金写入被拒绝（可重试错误）；链上观察/确认/修订继续或按可靠进度暂停（不得拒绝事实）；在途提款完成且事件齐备；`pending` 无「业务提交而事件缺失」缺口；不静默丢弃；恢复后补扫与排空。

### Q8 — 五态矩阵演练（双故障验收主场景）

1. 前置：本地确定性环境（Anvil + PG + Redis + Kafka + Compose）；执行 [verification.md](verification.md) §2 的 V-DRILL 步骤。
2. 状态：正常 → 仅 Redis 故障 → 仅 Kafka 故障 → 双故障（关 Redis+Kafka，保 PG+链）→ 恢复追赶；每态对七类操作按故障矩阵逐项核对。
3. 通过判定：矩阵一致率 100%；门禁绕过 0；恢复后事件补齐、进度恢复；0 重复提款意图/链上付款；0 权威状态丢失；0 孤儿充值永久入账；证据含模拟消费者入账幂等与验证边界声明（FR-16/SC-12）。

### Q9 — 追赶与再故障

1. 恢复后观察排空与消费者追赶；追赶中再次停 Kafka/Redis。
2. 通过判定：0 丢失、0 重复财务效果、进度可续；追赶时间可观测（数值待测）；再故障安全重暂停。

### Q10 — 对照基准

按 [adr.md](adr.md) §基准设计与 [verification.md](verification.md) §4 执行：相同负载/故障场景下分别运行 PG-only 与全栈路径，产出报告（p95/p99、吞吐、错误率、资源、追赶时间）；未测数值 0 次被表述为已达标。

### Q11 — 修订

1. 构造浅重组影响 Pending/Confirmed 充值；重复/乱序投递修订；同旧 `block_hash` 再 canonical。
2. 通过判定：修订事件携带旧身份/新状态或 Orphaned/原因/恢复版本；消费者收敛；0 次把 Orphaned 当有效 canonical；0 新付款意图/发送；复活与新观察不混淆。

## 4. 证据与报告

- 演练/基准产出的证据目录、报告模板与表述纪律见 [verification.md](verification.md) §2/§5；本节场景与 [plan.md](plan.md) SC 映射表对齐。
- 所有「通过」必须来自真实中间件与真实迁移执行；mock/double 不得作为验收证据引用（011 先例纪律）。
- 本文件不含实现代码、迁移与完整测试套件；实现细节属 tasks/implement。
