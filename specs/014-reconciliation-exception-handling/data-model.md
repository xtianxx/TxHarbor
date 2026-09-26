# Data Model: 014 Reconciliation and Exception Handling (Phase 1)

**Branch**: `014-reconciliation-exception-handling` | **Date**: 2026-09-26 | **Spec**: [spec.md](spec.md) | **Migration**: `migrations/000016_reconciliation_handling.sql` (new; actual max today `000015`)

Design only; no implementation in this round. All tables live in PostgreSQL (authoritative). No balance ledger. Money fields use integer/NUMERIC only.

## 1. Entities and Fields

### 1.1 `recon_task`（对账任务；FR-001/003/019/022, Q3）

- `task_id` UUID PK；`scope_chain_id` TEXT；`scope_kind` ENUM('height','time')；`scope_start/scope_end`（bigint/timestamptz，按 kind 其中之一有效）；`business_types` TEXT[]（闭集：withdrawal/deposit/event-delivery…，未知类型拒绝）；`policy_refs` JSONB（confirm policy_seq、cutover/catalog 版本快照）；`state` ENUM('created','running','paused','suspended_budget','done','cancelled')；`pause_reason` TEXT nullable；`budget` JSONB（并发/单次范围/时长/PG-RPC 配额）；`created_by/at`、`updated_at`。
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

## 2. 稳定身份与证据哈希

- 身份键 =（范围， 类别， 业务主键， 内容哈希， 证据版本域）。内容哈希覆盖三方快照规范化字节；版本域覆盖区块 number/hash、recovery/authorization/scope/state_version、证据时点。
- 同身份内容一致且无分歧 → 不建单（Q4）；同身份异内容/重复效果/版本违规 → 建单去重；证据不足 → `incomplete`。
- 重组/重扫不得丢失关联或无限建单：同身份重开原单，不同身份建关联单（`linked_to`）。

## 3. 认领/处置/闭合与并发保护

- 认领：`claimed` 需范围异常处理权限；单 owner；B 抢占同一 `open_claimable` 行用 `SELECT … FOR UPDATE` + 状态谓词 CAS，失败返回归属（US2-2）。
- 处置：需具体动作权限；`idempotency_key` 唯一冲突读回（011/013 `operation_conflict` 同形）；调用既有恢复入口时在同一 014 事务外另行满足其门禁（010 锁序、011 claim 验证、013 inbox/version 守卫、006 版本捕获），014 不代行授权。
- 闭合：需闭合权限 + 最新 `reverify=consistent` 且证据未过期；闭合写 `close_basis`（范围/区块/版本/时点快照）。
- 失效/重开：影响结论的并发写入/重组/新证据 → `pending_verify`（失效），确认再现 → `reopened`；无关写入不触发；历史闭合/重开原因与证据保留。

## 4. 证据包内容

- 三方快照引用（链：块号/哈希/回执效果/confirm basis；PG：行版本/状态/版本；事件：outbox 行/`consumer_progress`/`inbox`/`versions`/quarantine 行）；回执接入状态；扫描覆盖（checkpoint+gap）；新鲜度（各源 lag/age）；采集时间。缺任一项按 §2 保守分类。

## 5. 结果与检查点一致性及崩溃恢复

- 每批扫描提交：先持久化本批比较结果（差异/occurrence/复核行），再前移 `result_persisted_through`，最后追加 checkpoint 行——同一 DB 事务内完成；崩溃后从指针恢复：已持久前缀不重报，未完成区间继续，可重复扫描已覆盖区间（读+幂等写，无副作用）。
- 预算耗尽/暂停：停止领取新区间，在途有界完成或取消，留 gap 行，不前移指针越过未完成区间。
