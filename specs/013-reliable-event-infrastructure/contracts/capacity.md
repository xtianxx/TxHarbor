# Contract: Outbox Capacity Guard（PD-2 落地）

**Feature**: 013-reliable-event-infrastructure | **Plan**: [../plan.md](../plan.md) D7 | **Research**: R13 | **Spec**: FR-20；PD-2 | **Data**: [../data-model.md](../data-model.md) §6

## §1 指标与配置

**指标**（PG 查询，Redis 不参与）：`outbox_pending_count{class}`、`outbox_pending_oldest_age_seconds{class}`（class = event_type 族）；软/硬边界触发计数 `capacity_soft_breaches_total`、`capacity_hard_breaches_total`、`capacity_refusals_total{op_class}`。

**配置键**（fail-closed，数值待测）：`soft_limit`、`hard_limit`、`reserve`、`retention`、`max_shutdown_window`、`drain_target_window`。

**公式**（输入实测，见 §5）：`reserve = E_max × F_max × safety_factor`；`soft_limit = reserve + D × W`；`hard_limit = soft_limit + reserve`。启动校验 `0 < reserve < soft_limit < hard_limit`，不满足拒绝启动。

## §2 「在途可完成」与「Outbox 已满」不矛盾（不变量 I-CAP）

1. **判定时点**：容量保护只作用于**接纳新可控制工作之前**（新提款创建等可控 intake）与链上处理的**开始/续跑时点**；已接纳单元不再被容量拒绝。
2. **原子提交**：每个已接纳单元的业务状态与必需事件行在 T1 中同事务提交（[../data-model.md](../data-model.md) §5），接纳即保证事件行可写，无「已提交状态无事件」路径。
3. **预留覆盖写放大**：`reserve ≥ E_max × F_max`，任一时刻在途单元全部完成所需的事件行总量不超过预留；`pending ≤ hard_limit` 时仍有空间完成在途。
4. **达到上限的来源**：只可能是「不可拒绝」的链上事实（此时暂停处理、恢复后补扫，而不是拒绝事实）与已被软边界拒绝之前的存量接纳；不存在「可控新工作在满仓时被接纳」的路径。
5. 故「在途可完成」与「Outbox 已满」描述不同时点/不同工作类别，不矛盾。测试断言：容量触发后已接纳操作 100% 完成且事件齐备；`pending` 不出现业务提交而事件缺失的缺口。

## §3 行为矩阵（实现 MUST 遵循）

| 状态 | 可控制新资金写入 | 链上观察/确认/修订 | 在途提款 | 非关键功能 | 事件 |
|---|---|---|---|---|---|
| `pending < soft_limit` | 正常 | 正常 | 正常 | 正常 | 正常发布 |
| `soft_limit ≤ pending < hard_limit` | **拒绝**（明确可重试错误，[redis.md](redis.md) §3 同通道）；`capacity_refusals_total` | 正常（预留容量保护） | 正常（不新建意图） | 降级 | 正常发布（追赶） |
| `pending ≥ hard_limit` | 拒绝 | 无法安全持久化时从 003/004 可靠进度**暂停处理**、容量恢复后**补扫**；MUST NOT 跳过观察、MUST NOT 拒绝链上已发生事实 | 按既有暂停/对账协议收尾；结果未知先对账；不新建付款意图 | 暂禁 | 恢复后发布 |

红线：MUST NOT 静默丢弃事件、MUST NOT 覆盖未发布事件、MUST NOT 删除 `pending|blocked`、MUST NOT 让缓存/Redis 参与判定、MUST NOT 因容量问题放宽任何上游门禁（PD-2/FR-20/SC-09）。

**hard_limit 语义（准确表述）**：`hard_limit` 是**准入闸 + 暂停触发器**，不是物理容量上限，也不承诺存储永不耗尽。已接纳单元的写入 MUST NOT 因容量被拒绝（I-CAP，§2）；链上已发生事实 MUST NOT 被拒绝，因此 `pending` 可以超过 `hard_limit`：持久化仍安全时不可拒绝的链上事实继续入 Outbox；无法安全持久化（含物理存储/写入能力受限）时从可靠进度暂停、恢复后补扫。MUST NOT 把 `hard_limit` 表述为物理容量上限，也不得暗示物理容量不会耗尽（物理存储真正耗尽属于「无法安全持久化」，按暂停/补扫处置，绝不静默丢弃）。

## §4 验收断言（Fault/Integration 层）

- 停机注入期间：已提交事件丢失数 0；`outbox_pending_count`/`oldest_age` 可观测率 100%。
- 软边界：可控新写入 100% 拒绝且返回可重试错误；存量/链上/在途行为与 §3 一致；0 门禁绕过。
- 硬边界（构造）：从可靠进度暂停与恢复后补扫，观察数 0 丢失、0 跳过。
- 恢复：排空完成，追赶时间可观测；排空窗口数值待测（不编造）。

## §5 阈值测量方法（不编造数值）

1. `E_max`：对每个接纳型操作类，在 Integration/E2E 中统计单事务可插入事件行数上界（含批量路径的最坏值）。
2. `F_max`：由并发模型（worker 数、HTTP 并发上限）实测并发在途峰值。
3. `D`：Kafka 健康时以受控负载测发布器排空速率（事件/秒），取稳定段中位数与保守下界。
4. `W`：由业务/运维选择的目标排空窗口（配置项，可裁决）；无裁决时用 `max_shutdown_window` 推导。
5. `safety_factor`：按 `D` 的方差与测量误差取保守上界（如 P95 最坏速率反推），实现批次固化。
6. 告警阈值：≥ soft 持续时长超配置窗口告警；到达 hard 立即告警（P1）。

配置边界：所有键必须有显式值或安全默认；缺省不提供「无界」语义；修改需重启（配置 fail-closed），运行中阈值变更须走配置发布流程并记录（实现批次）。
