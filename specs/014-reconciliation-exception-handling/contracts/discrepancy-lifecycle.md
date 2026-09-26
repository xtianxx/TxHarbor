# Contract: Discrepancy Lifecycle (014)

**Spec**: [spec.md](../spec.md) (FR-007/008/009/010/012/013/015/016/017/018, Q1/Q4/Q5) | **Data**: [data-model.md](../data-model.md) §1.4–1.8/§2–3

## States

`open_claimable → claimed → disposing → pending_verify → closed`（+ `reopened → pending_verify`；驳回/转人工为显式边）

## Transitions

- 检出建单（仅业务分歧；Q4）：`open_claimable` + 证据包；幂等吸收零分歧不建单。
- `claim(owner)`：单 owner CAS；B 抢占返回归属并审计。
- `dispose(kind, action_ref, idempotency_key)`：`ack_only` 默认；`reuse_recovery` 仅引用既有入口（014 不自动执行）；`new_fix_rule` 仅 `dry_run`。重复 key 读回收敛。
- 系统复核写 `reverify` 行；`consistent` 仅证据完整新鲜且规则满足。
- `close`: 需闭合权限 + 最新 consistent 未过期；写 `close_basis`（范围/区块/版本/时点）。
- 失效/重开：并发变化/重组/新证据 → `pending_verify`；确认再现 → `reopened`（`reopen_count+1`，历史保留）；无关写入不触发；过期结果不得闭合。
- 非法跳转拒绝并审计。差异 MUST NOT 解释为重付许可（FR-015）。

## Classification (machine)

`missing | duplicate_divergent | state_mismatch | unknown | incomplete`；未知形状 → `incomplete` 只告警；证据不足 → 待核验/覆盖不完整。

## History Revalidation Sweep（F4；与 `data-model.md` §6 一致）

- 发现者：扫描主循环顺带 cross-check（落入当前预算区间的已闭合项）＋ 定向复查枚举（本任务范围内 `closed` 且 `close_basis` 版本域落后者，按证据年龄最旧优先）。
- 触发与预算：每次扫描调用预留有界 slice；重组/frontier 推进仅作下次优先提示；消耗计入任务总预算。
- 进度：`history_sweep_through` 前进；未覆盖留 gap；无无限全量扫描。
- 验收锚点见 quickstart §11：水位前进＋旧范围证据变化＋无人工逐条触发 → 进入重验证；中断恢复与证据不足不得错误闭合。
