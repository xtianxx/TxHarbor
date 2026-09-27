# Contract: Discrepancy Lifecycle (014)

**Spec**: [spec.md](../spec.md) (FR-007/008/009/010/012/013/015/016/017/018, Q1/Q4/Q5) | **Data**: [data-model.md](../data-model.md) §1.4–1.8/§2–3

## States

`open_claimable → claimed → disposing → pending_verify → closed`（+ `reopened → pending_verify`；驳回/转人工为显式边）

## Transitions

- 检出建单（仅业务分歧；Q4）：`open_claimable` + 证据包；幂等吸收零分歧不建单。
- `claim(owner)`：单 owner CAS；B 抢占返回归属并审计。
- `dispose(kind, action_ref, idempotency_key)`：`ack_only` 默认；`reuse_recovery` 仅引用既有入口（014 不自动执行）；`new_fix_rule` 仅 `dry_run`。重复 key 读回收敛。
- 系统复核写 `reverify` 行；`consistent` 仅证据完整新鲜且规则满足。
- 生产复核入口（`reconcile-admin reverify-ticket`，2026-09-27 补齐）：对 `pending_verify` 票做一次有界、受任务 scope 的 scan-management 授权（auth-matrix `reverify` 行仍 system-only）的全量三路再比较，复用真实 chain/PG/event 适配器与 R1/R2/R3 期望判别；仅三方完整、新鲜、覆盖闭合且记录结论守卫（区块身份、记录 recovery 版本、聚合成员完整性）通过时写 `consistent`，其余写 unknown/stale/divergent + gap；裁决在票行锁内按**复核有效性令牌**协议写入（见下节），令牌不符则丢弃并审计；不放宽处置/闭合/恢复/付款门禁。
- `close`: 需闭合权限 + 最新 consistent 未过期；写 `close_basis`（范围/区块/版本/时点）。最新 `reverify` 行在**票行锁内**读取（与状态 CAS 同一事务，latest-row-wins），旧结果不得覆盖新裁决。

## Revalidation Token Protocol（复核有效性令牌；T026/T027）

写写反序保护，适用于单票入口（`ticketverify.go`）、历史 sweep（`reverify.go persistReverifyOutcomeTx`）与所有失效/状态写入（`lifecycle.go TransitionDiscrepancy`、扫描聚合证据替换）：

- **捕获（取证前）**：`(reverify_generation, state, hash(evidence_version_domain))` 在物化待复核项的同一读取中捕获；`reverify_generation` 由每一次被接受的裁决写入与每一次 ticket 行变更推进。
- **有界取证（事务外）**：证据重读不持有事务/行锁；时长与尝试次数有界。
- **提交校验（共同锁内）**：在 discrepancy 行锁（与 close 同一把锁）下重读并逐项校验令牌；不符即丢弃，只写 `result=discarded` 审计，不写裁决行（不插替代 unknown）、不增删 gap、不推进游标/计数。
- **时序保护不使用** `created_at`、进程时钟或裁决类型；`consistent` 覆盖 `consistent` 同样被拒。最新行读取以提交序（`reverify_id`）为同时间戳的裁决序。
- 失效/重开：并发变化/重组/新证据/来源或版本轮换 → `pending_verify`（仅触发已批准的重验证流程，不扩大为自动处置）；确认再现 → `reopened`（`reopen_count+1`，历史保留）；无关写入不触发；过期结果不得闭合。
- 升级与回滚：旧实现与本协议**不得混跑**；检查清单见 `quickstart.md` §call-path「000018 升级与回滚」（回滚到旧实现即失去反序保护）。
- 非法跳转拒绝并审计。差异 MUST NOT 解释为重付许可（FR-015）。

## Classification (machine)

`missing | duplicate_divergent | state_mismatch | unknown | incomplete`；未知形状 → `incomplete` 只告警；证据不足 → 待核验/覆盖不完整。

- 事件维度的期望判别（Q-cutover 已裁决 B，2026-09-27）：标记与事件同时缺失、旧生产者无标记 → 保守 `pending`（标记缺席≠N/A）；已有充分证据不因缺少新标记被无条件降级（祖父规则）；「证据不足」告警是 `pending` 上的可观察信号，不是确定 missing。规则与实现指向见 [expected-event-discriminator.md](../expected-event-discriminator.md) §2–§3（待实现，T040）。

## History Revalidation Sweep（F4；与 `data-model.md` §6 一致）

- 发现者：扫描主循环顺带 cross-check（落入当前预算区间的已闭合项）＋ 定向复查枚举（本任务范围内 `closed` 且 `close_basis` 版本域落后者，按证据年龄最旧优先）。
- 触发与预算：每次扫描调用预留有界 slice；重组/frontier 推进仅作下次优先提示；消耗计入任务总预算。
- 进度：`history_sweep_through` 前进；未覆盖留 gap；失败项记 gap（`query_failed`）后水位可继续推进但不得宣称已验证完整；gap 按失败次数升序＋证据年龄降序有界重试，成功即消除；无无限全量扫描。被写写反序协议丢弃（`discarded`）的项不算进度：不推进游标、不计入 rechecked/裁决计数，`verified_complete` 要求零丢弃。
- 验收锚点见 quickstart §11：水位前进＋旧范围证据变化＋无人工逐条触发 → 进入重验证；中断恢复与证据不足不得错误闭合。
