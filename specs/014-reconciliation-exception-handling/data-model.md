# Data Model: 014 Reconciliation and Exception Handling (Phase 1)

**Branch**: `014-reconciliation-exception-handling` | **Date**: 2026-09-26 | **Spec**: [spec.md](spec.md) | **Migration**: `migrations/000016_reconciliation_handling.sql` (new; actual max today `000015`)

Design only; no implementation in this round. All tables live in PostgreSQL (authoritative). No balance ledger. Money fields use integer/NUMERIC only.

## 1. Entities and Fields

### 1.1 `recon_task`（对账任务；FR-001/003/019/022, Q3）

- `task_id` UUID PK；`scope_chain_id` TEXT；`scope_kind` ENUM('height','time')；`scope_start/scope_end`（bigint/timestamptz，按 kind 其中之一有效）；`business_types` TEXT[]（闭集：withdrawal/deposit/event-delivery…，未知类型拒绝）；`upstream_receipt_source` JSONB（每业务类型的上游回执来源与接入状态：`{business_type: {source, connected}}`，`connected=false` 即未接入；创建时由任务参数写入，运行中仅经任务修订事务变更）；`policy_refs` JSONB（confirm policy_seq、cutover/catalog 版本快照）；`state` ENUM('created','running','paused','suspended_budget','done','cancelled')；`pause_reason` TEXT nullable；`budget` JSONB（并发/单次范围/时长/PG-RPC 配额，含历史复查 slice 见 §5）；`history_sweep_through` JSONB nullable（历史复查水位，见 §6）；`created_by/at`、`updated_at`。
- Validation: 范围必填可复现；空范围输出“覆盖为空且完整”需任务行 + 零 checkpoint 跨度共同证明（Edge）。

### 1.2 `recon_checkpoint`（检查点；FR-003, Q3-2/Q5）

- `task_id` FK；`seq` BIGINT（单调）；`covered_through`（高度/时间，与 scope 同 kind）；`result_persisted_through`（结果已持久化水位，MUST ≤ covered_through 的已完成前缀）；`created_at`。
- UNIQUE(`task_id`,`seq`)；`task` 当前指针 = MAX(seq)。取消/超时/失败 MUST NOT 前移指针越过未完成区间；恢复可重复扫描已覆盖区间（幂等）。

### 1.3 `recon_gap`（未覆盖区间；FR-003/019, Q3-5）

- `task_id` FK；`range_start/range_end`；`reason` ENUM('not_started','interrupted','budget_exhausted','paused','freshness_hold','upstream_unconnected','query_failed')；`created_at`。
- 暂停/超预算/不完整 MUST 留 gap 行；“全量一致”结论要求零开放 gap。

### 1.4 `discrepancy`（差异；FR-007/009/010, Q4/Q5）

- `discrepancy_id` UUID PK（稳定身份，见 §2）；`category` ENUM('missing','duplicate_divergent','state_mismatch','unknown','incomplete')；`business_key` TEXT（request_id/intent_id/event 身份等）；`content_hash` BYTEA；`evidence_version_domain` JSONB（范围/区块身份/业务版本/证据时点）；`state` ENUM('open_claimable','claimed','disposing','pending_verify','closed','reopened')；`claim_owner/claimed_at`；`close_basis` JSONB nullable；`reopen_count` INT default 0；`linked_to` UUID nullable（不同身份关联单）；`created_at/updated_at`。
- 保守默认：未知形状 → `incomplete` 只告警（Edge）。幂等吸收零分歧 MUST NOT 建单（Q4）。

### 1.5 `discrepancy_occurrence`（重复发生记录；FR-007/SC-002, Q4/Q5）

- `discrepancy_id` FK；`observed_at`；`evidence_ref`；`scan_task_id` FK。重复检出追加行，不建新单；同一身份重开沿用原单（`reopen_count+1`）。

### 1.6 `disposition`（处置记录；FR-012/013/016, Q1/Q2）

- `disposition_id` UUID PK；`discrepancy_id` FK；`kind` ENUM('ack_only','reuse_recovery','new_fix_rule')；`action_ref` TEXT（既有入口引用，如 `txlifecycle.UnknownRecovery`/`events-admin replay`，仅引用不自动执行）；`operator` TEXT；`reason` TEXT；`evidence_ref`；`result` ENUM('done','refused','failed','dry_run')；`idempotency_key` TEXT UNIQUE（重复处置幂等收敛）；`created_at`。
- `new_fix_rule` 本阶段仅允许 `dry_run`（FR-023 已裁决不批准）。

### 1.7 `reverify`（复核结果；Q1/Q5）

- `discrepancy_id` FK；`verdict` ENUM('consistent','divergent','unknown','stale')；`evidence_ref`；`freshness_at`；`created_at`。`consistent` 仅当证据完整新鲜且满足一致性规则；超时/不完整/不可用/不足 MUST NOT 判 consistent。自动重验证仅写本表 + 014 自身记录（Q5-6）。

### 1.8 `recon_audit`（审计轨迹；FR-012, Q2）

- Append-only；`audit_id` BIGSERIAL PK；`actor`；`action` ENUM('query','start','pause','resume','claim','dispose','reverify','close','reopen','refuse')；`target` JSONB；`reason/evidence/result`；`created_at`。越权拒绝亦记行（含归属提示）。

### 1.9 `recon_scan_attempt`（扫描认领；F3 领取—执行—提交协议载体）

- `attempt_id` UUID PK；`task_id` FK；`range_start/range_end`（本次认领区间，由指针 + 预算在短事务内计算）；`state` ENUM('claimed','done','abandoned','superseded')；`owner` TEXT（调用者身份）；`lease_expires_at` TIMESTAMPTZ（心跳可选，超时即放弃候选）；`created_at/updated_at`。
- UNIQUE(`task_id`,`attempt_id`)；同一任务同一区间只允许一个 `claimed` 行（部分唯一索引 `WHERE state='claimed'`）。
- 认领短事务：`SELECT recon_task … FOR UPDATE` 校验 state=running → 计算区间 → INSERT attempt → COMMIT，全程无 RPC；行锁仅存续于该短事务，不跨慢调用。

### 1.10 `recon_permission`（对账权限注册表；F5 求值源）

- `principal` TEXT；`action` ENUM('scan_manage','exception_handle','dispose_ack','dispose_reuse','close')；`scope` JSONB（链/业务类型/范围前缀）；`granted_by/at`；PK(`principal`,`action`,(`scope` 规范化哈希))。
- 默认拒绝：无行即无权；未知动作、越界范围一律拒绝并审计。授予操作为部署期运维行为，本阶段不预置任何授予（机制已定，政策未裁决）。
- 信任根与自举（管理授权裁决，仅 014，2026-09-26，已决）：授予/撤销走本地特权操作路径（`contracts/auth-matrix.md` Management 节），`principal` 绑定认证调用者身份；普通持有者不得自授；首次信任根经受控部署配置建立（身份绑定＋可管范围＋审计），无有效配置默认拒绝；单人执行，本阶段不强制第二人审批；未设管理员角色，未扩大任何既有权限。

## 2. 稳定身份与证据哈希

- 身份键 =（范围， 类别， 业务主键， 内容哈希， 证据版本域）。内容哈希覆盖三方快照规范化字节；版本域覆盖区块 number/hash、recovery/authorization/scope/state_version、证据时点。
- 同身份内容一致且无分歧 → 不建单（Q4）；同身份异内容/重复效果/版本违规 → 建单去重；证据不足 → `incomplete`。
- 重组/重扫不得丢失关联或无限建单：同身份重开原单，不同身份建关联单（`linked_to`）。

## 3. 认领/处置/闭合与并发保护

- 认领：`claimed` 需范围异常处理权限；单 owner；B 抢占同一 `open_claimable` 行用 `SELECT … FOR UPDATE` + 状态谓词 CAS，失败返回归属（US2-2）。
- 处置：需具体动作权限；`idempotency_key` 唯一冲突读回（011/013 `operation_conflict` 同形）；调用既有恢复入口时在同一 014 事务外另行满足其门禁（010 锁序、011 claim 验证、013 inbox/version 守卫、006 版本捕获），014 不代行授权。
- 闭合：需闭合权限 + 最新 `reverify=consistent` 且证据未过期；闭合写 `close_basis`（范围/区块/版本/时点快照）。
- 失效/重开：影响结论的并发写入/重组/新证据/来源或版本轮换 → `pending_verify`（失效，仅触发已批准的重验证流程，不扩大为自动处置），确认再现 → `reopened`；无关写入不触发；历史闭合/重开原因与证据保留。

## 4. 证据包内容

- 三方快照引用（链：块号/哈希/回执效果/confirm basis；PG：行版本/状态/版本；事件：outbox 行/`consumer_progress`/`inbox`/`versions`/quarantine 行）；回执接入状态；扫描覆盖（checkpoint+gap）；新鲜度（各源 lag/age）；采集时间。缺任一项按 §2 保守分类。

## 5. 结果与检查点一致性及崩溃恢复

- 每批扫描提交：先持久化本批比较结果（差异/occurrence/复核行），再前移 `result_persisted_through`，最后追加 checkpoint 行——同一 DB 事务内完成；崩溃后从指针恢复：已持久前缀不重报，未完成区间继续，可重复扫描已覆盖区间（读+幂等写，无副作用）。
- 预算耗尽/暂停：停止领取新区间，在途有界完成或取消，留 gap 行，不前移指针越过未完成区间。

### 5.1 领取—执行—提交协议（F3；互斥范围＝认领行，不跨慢调用持锁）

- 领取（短事务，无 RPC）：`SELECT recon_task … FOR UPDATE` 校验 `state='running'` 且预算充足 → 按指针 + 预算计算区间 → INSERT `recon_scan_attempt(state='claimed')` → COMMIT。行锁仅存续该短事务；事务外读取期间所有权由 attempt 行（`task_id` + `attempt_id` + 未过期 lease）证明，不靠长事务。
- 执行（事务外）：持 attempt 身份做 RPC 与只读查询；可中断/取消（检查任务 state，暂停即停领，在途有界收尾）。
- 提交（短事务）：重读任务行（FOR UPDATE）+ attempt 行；仅当 attempt 仍为当前有效认领（未被暂停/取消/超期取代）才持久化结果并按连续已持久前缀前移指针；迟到执行者（attempt 已 superseded/abandoned）提交被拒绝，结果丢弃并审计，不覆盖指针。
- 崩溃恢复：重启后 `claimed` 且 lease 过期的 attempt 置 `abandoned` 并留 gap 行，指针不动；续扫重领。检查点条件更新：指针只前移到连续已持久前缀的最大值。
- 反例覆盖：双调用者同时领取同一任务（T025 并发反例）→ 仅一 attempt 落为 claimed，另一因部分唯一索引/状态谓词失败重试下一区间；慢 RPC 期间不持有 DB 事务。

## 6. 历史复查（F4；覆盖新水位之前的已闭合项）

- 发现者：扫描主循环（顺带 cross-check 落入当前预算区间的已闭合项）＋ 定向复查枚举（按证据年龄最旧优先，从 `discrepancy` 中选取本任务范围内 `closed` 且 `close_basis` 版本域落后于当前源版本的项）。
- 触发：每次扫描调用预留预算 slice（配额内固定小比例，上限有界）执行历史复查；重组/ frontier 推进信号仅作为下次调用优先复查的提示，不另起通道。
- 范围与进度：复查范围限任务 scope 内已闭合项；进度以前进的 `history_sweep_through`（证据年龄水位）记录在任务行；未覆盖部分留 gap（reason=`interrupted` 或预算耗尽）。
- 失败与公平推进：单项复查失败（查询失败、源不可用、超时）记 gap 行（reason=`query_failed`，附证据年龄与失败类），不阻塞水位——水位可越过已记 gap 项继续推进，但**遍历游标≠已验证完整**：被越过的 gap 项不得据此宣称已验证完整，其相关差异不得闭合；gap 项按“失败次数升序＋证据年龄降序”在后续 slice 内有界重试（单项重试次数上限有界，耗尽后保持 gap 可见并升级告警，不无限占用 slice，避免后续项饥饿）。
- gap 生命周期：复查成功即消除对应 gap 行；证据过期使复查结论只能为 `stale`/待验证；中断恢复后从水位继续，已记 gap 保留；成功消除、过期、重试上限三态均审计。
- 预算：复查消耗计入任务总预算 slice，不挤占新区间扫描主配额之外；无无限全量扫描；不编造生产时限数值。
- 验收锚点：水位已前进＋旧范围证据变化＋无人工逐条触发 → 差异进入重验证（quickstart §11）；中断恢复与证据不足不得错误闭合（沿用 §5 与 Q5-4）。
