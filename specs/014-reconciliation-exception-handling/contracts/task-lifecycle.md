# Contract: Task Lifecycle (014)

**Spec**: [spec.md](../spec.md) (FR-001/003/004/005/006/019/022, Q3) | **Data**: [data-model.md](../data-model.md) §1.1–1.3/§5

## States

`created → running ⇄ paused | suspended_budget → running → done | cancelled`

- `created`: 范围/预算/策略引用已记录，未领取扫描工作。
- `running`: 按预算领取区间并提交结果+检查点（同事务）。
- `paused`: 按授权范围暂停；不领新工作；在途有界完成或取消；留 gap 行。
- `suspended_budget`: 配额耗尽；可观察等待；留 gap 行。
- `done`: 范围闭合且零开放 gap；结论可为一致或差异清单。
- `cancelled`: 终止；指针不越过未完成区间。

## Operations (admin command surface; auth in auth-matrix.md)

- `start(scope, budget)` → `created`; `pause(task, reason)` / `resume(task)` 仅影响 014 授权范围任务；`cancel(task)` 不前移指针。
- 策略不可修订（T037，2026-09-26 收口核定）：`policy_refs`/scope/budget 创建时确定，无修订事务；变更走 `cancel` + 重建新任务；`policy_refs` 缺失旧任务链证据恒 pending（fail-closed），禁止静默默认值；`--confirm-threshold-n` 为对账观察参数（来源=start 显式参数，快照=`task.policy_refs`），不写 `confirmation_policy_history`，不改变 005 确认语义。
- 非法跳转拒绝并审计。暂停/超预算 MUST NOT 显示“全量一致”；新增数据缺口须可见（Q3-5）。

## Integrity Rules

- checkpoint 行仅覆盖已完成且已持久化前缀；cancel/timeout/failure 不越界；恢复可重扫。
- 预算字段有界；耗尽停新工作，不忙循环不无界重试；记录原因/指针/gap/新鲜度。

## Claim–Execute–Commit Protocol（F3；与 `data-model.md` §5.1 一致）

- 互斥保护范围＝`recon_scan_attempt` 认领行（同一任务同一区间单 `claimed`），不以领取短事务的行锁冒充全程互斥。
- 领取短事务（无 RPC）→ 事务外执行（凭 attempt 身份）→ 提交短事务（条件更新指针，迟到者丢弃并审计）。
- 崩溃后过期 attempt 置 `abandoned` 并留 gap，指针不动；检查点只前移到连续已持久前缀最大值。
- 慢 RPC 期间不持有 DB 事务；并发反例见 tasks T025。
