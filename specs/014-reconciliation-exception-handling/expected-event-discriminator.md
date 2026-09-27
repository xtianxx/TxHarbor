# 期望事件判别器（Expected-Event Discriminator）最小设计

- Feature: `014-reconciliation-exception-handling`
- 状态: **设计已定（Q-cutover 已裁决选项 B），未实现**（2026-09-27 文档收口＋裁决记录轮）；本文只定义判据、证据载体与政策裁决结果，不改产品代码与 CI
- 关联任务: T030（保持未勾选；关闭依赖 T040）、T040（实现；政策阻塞已随 Q-cutover 裁决 B 解除，见 §4）；CI 分类缺口见 T039
- 关联证据: `docs/evidence/014/quickstart_matrix_evidence.md` §6（T030 第三项 BLOCKED）、§10（Q-cutover 与告警语义裁决记录）

## 1. 契约依据（已核实）

### 1.1 事件目录 v1（封闭 8 类型）

013 事件目录 v1 在 `internal/events/catalog.go:142-193` 封闭声明 8 个类型（契约：`specs/013-reliable-event-infrastructure/contracts/events.md` §3「目录 v1（catalog_version = 1）」）。生产侧 `events.Append` 共 8 处，全部与业务转换同事务提交：

| 目录类型 | 生产侧原子 Append（`events.Append`） | 业务转换调用点（同事务） |
|---|---|---|
| `deposit.observation.created` | `internal/indexer/outbox_events.go:104` | `internal/indexer/depositcommit.go:343`（新插入 RowsAffected=1 时） |
| `deposit.observation.status_changed` | `internal/indexer/outbox_events.go:146` | `internal/indexer/reorgcommit.go:588` |
| `deposit.observation.reinstated` | `internal/indexer/outbox_events.go:279` | `internal/indexer/reorgcommit.go:1310` |
| `deposit.confirmation.confirmed` | `internal/indexer/outbox_events.go:190` | `internal/indexer/confirmcommit.go:387` |
| `deposit.revision.applied` | `internal/indexer/outbox_events.go:350` | `internal/indexer/reorgcommit.go:599`（失效）与 `:1315`（复活） |
| `withdrawal.request.received` | `internal/withdrawal/intake.go:226` | `internal/withdrawal/intake.go:498` |
| `withdrawal.execution.state_changed` | `internal/execution/intent.go:206` | `internal/execution/intent.go:228-249` |
| `withdrawal.execution.revised` | `internal/execution/revision.go:370` | `internal/execution/revision.go:327-374` |

- 原子性：Append 与业务转换同一事务提交，任一失败整体回滚，无半提交状态、无脱离业务事实的事件（上述调用点注释即 013 T1 语义）。
- 「8 处」指目录类型数与生产 Append 站点数；`deposit.revision.applied` 有失效/复活两条业务路径，故业务转换调用点多于 8 处。`internal/faultdrill/harness.go:508` 属 `//go:build fault` 演练支持，不计入生产生产者。

### 1.2 已证明不在覆盖范围（不产生目录事件）

- 011 admission：`internal/execution/admit.go:154-176` 写 `payment_intents` INSERT＋011 `execution_events`（`EventAdmitted`）；`:214-229` 的 `admission_refused` 同样只写 011 `execution_events`；均无目录类型。
- 006 recovery：`internal/indexer/reorgcommit.go:606` 的 `appendRecoveryEvent`（定义 `:260-272`）写 006 `recovery_events`，不是目录事件。
- 幂等重放 0 追加：重复执行插入 0 行/转换 0 行时追加 0 事件（`internal/indexer/depositcommit.go:337-347`；`internal/indexer/reorgcommit.go:575-578`、`:1300-1304`）。
- cutover 前历史无事件：`migrations/000015_event_infrastructure.sql:222-223` 在迁移时以 `now()` 写入 `event_system_state.cutover_at`；迁移与代码无历史事件回填（`internal/events/audit.go` 明确 audit 不回写、不静默回填）；旧实体无 head 事件时不伪造 revision（`reorgcommit.go:593-597`，`:1314` 的 `supersedes` 守卫）；消费者首见版本即基线（`internal/events/consumer.go:555`；013 `specs/013-reliable-event-infrastructure/contracts/events.md` §7、`specs/013-reliable-event-infrastructure/data-model.md` §7「不伪造历史事件」）。

### 1.3 014 现状（缺口）

- event-only 缺席在 complete 覆盖下一律 fail-closed `missing`：`internal/reconciliation/classify.go:516-526`（Chain Present 且 PG/Event Absent → `CategoryMissing`）；`internal/reconciliation/scan.go:1727`（`eventUsable = CoverageClosed`）与 `:1798-1820`（仅 `eventUsable` 时 Event 才可为 Absent，否则 Unknown）。
- 无 cutover 判据载体：`internal/reconciliation/` 包内无任何 `cutover` 读取（仅注释提及）；`internal/reconciliation/eventstate.go:2461-2470` 的 outbox 读取（`event_state_outbox_where`）无 cutover 过滤。
- 无 expected-event registry：`internal/reconciliation/taskadmin.go:121-141` 的 `policy_refs` 实测仅校验 `confirm_threshold_n`，无期望事件声明位。
- `event_system_state.cutover_at` 仅证明**迁移时刻**，不等于**生产者 rollout 时刻**：013 契约即写明「应用迁移后 `event_system_state.cutover_at = now()`；代码上线后所有目录转换开始发射」（013 `specs/013-reliable-event-infrastructure/data-model.md` §7）。现有探针以业务时间近似（`internal/app/eventpublisher.go:230-243` 以 `created_at`/`admitted_at` ≥ `cutover_at` 过滤；`internal/indexer/audit_source.go:42-45` 以 `observed_at` ≥ `cutover_at` 过滤），只是近似；**不得**据此用业务 `created_at`/`observed_at` 或区块时间反推「当时应有事件」。

## 2. 三路裁决

判别器对每个（实体转换 × 事件类型）义务给出且仅给出三类裁决；前提是覆盖完整（`CoverageClosed`）。覆盖不完整时走现有 incomplete/gap 路径，不进入下列裁决。

| 裁决 | 前提（期望证明） | 事件证据 | 结果 |
|---|---|---|---|
| R1 应有而缺失 → `missing` | 已证明该转换有事件义务（期望标记/等效证明） | 覆盖完整且事件缺席 | 保留现行 fail-closed missing 票 |
| R2 不在覆盖范围 → 该事件维度 N/A | 已证明该转换无事件义务（如 §1.2 各类） | 不适用 | 不宣称三方一致；链/PG 差异独立成立，**不因事件未知而降级** |
| R3 是否应有无法证明 → `pending`/gap | 期望无法证明（旧数据无标记、跨边界、证据版本不确定等） | 任意 | 既不判 missing 也不判一致；`pending` 常驻可观察，可产生「证据不足」告警（告警 ≠ 确定 missing） |

- 跨边界区间：义务时间/区块落在区间边界时按区间拆分处理；任一侧不完整即整体 pending/gap。
- 旧实体新转换：旧实体产生新转换本身构成新义务（rollout 后），按 R1/R3 判；不得用实体年龄推断无义务。
- 重组/修订与证据版本变化：走 Q5 既有 `pending_verify` 失效路径（`internal/reconciliation/lifecycle.go`），不新造裁决类别。
- **标记缺席 ≠ N/A**：标记与事件同时缺失、旧生产者未写过标记时，一律保守 `pending`（R3）；不得因「没有标记」把该事件维度判为 N/A。R2 只适用于**已证明**不适用（如 §1.2 各类）。
- **祖父规则**：已有充分证据的义务（§1.1/§1.2 级别的现有原子性/覆盖证据）不得仅因缺少新期望标记而被无条件降级；有充分证据且事件缺失仍判 `missing`（R1），已证明不适用仍判 N/A（R2）。
- **告警语义（消除 pending vs alert-only 歧义）**：「证据不足」告警是 `pending` 上的可观察信号，不是第二种确定结论；`pending` 常驻可观察；不得判一致、不得闭合。

## 3. 最小持久证据设计方向（未实现）

- 生产者期望标记：在业务转换同一事务内原子写入「该转换应产生某事件」的标记（建议新表如 `event_obligation`：aggregate 三元组＋`expected_event_type`＋`obligated_at`＋`source`，复用 Append 同事务模式；或等效载体）。标记本身即期望证明，供 014 读路径查询。
- 能力边界（2026-09-27 裁决）：期望标记是设计方向，但**不能独立证明所有生产者路径都正确生成了事件**；实现时必须由生产者侧原子性证据（同事务提交/回滚/幂等的既有机制与测试证据）补足，该证明属工程事项，不在本轮证明范围。
- 旧行无标记＝unknown：不得推断「应有」或「不应有」。
- 权限：标记的写入/修订沿用各义务方现有事务权限（与对应业务转换同权限），不新增越权面、不新增管理面。
- 旧任务兼容：无标记一律 `pending`；存量 missing 票不静默关闭，进入待重验证；新证据到达后按 R1/R2 重判。
- 失效规则：重组/版本轮换/来源变化 → `pending_verify`（复用 Q5 既有失效规则）。
- 分界：**工程机制**＝标记的原子写入与读路径三路裁决实现（T040）；**业务政策**＝哪些转换有事件义务、期望义务目录变更（§6）。Q-cutover 已裁决 B，机制按本文方向实现，不再等待政策批准。

## 4. 政策裁决记录（Q-cutover 已裁决选项 B；告警语义已定）

### Q-cutover：已裁决（选项 B，2026-09-27）

用户裁决原文要点（逐字落实）：

- 选项 B：持久、可追溯的事件预期证据。
- **不批准**迁移时间、业务时间、区块时间或未经证明的部署时间的近似判定。
- 消除「接受近似」歧义：Q-cutover 以选项 B 关闭；`event_system_state.cutover_at` 仅作审计参考，不参与裁决（选项 A 的近似判据不获批准）。
- 同事务期望标记可作设计方向，但不得声称它能独立证明所有生产者路径都正确生成了事件（能力边界见 §3；实现时由生产者侧原子性证据补足，为工程事项）。
- 不伪造历史标记；不回填未经证明的 cutover（边界规则见 §5）。

### 告警语义：已定（消除「pending vs alert-only」歧义）

- `pending` 常驻、可观察；「证据不足」告警是 `pending` 上的可观察信号，不是第二种确定结论。
- 不得判一致、不得闭合；后续有效证据可重判（见 §5）。

## 5. 边界规则、任务映射与关闭条件

### 边界规则（2026-09-27 裁决补足）

- 历史未知不静默忽略：保持可观察、可复查的 `pending`/gap；后续有效证据可重判；不要求永远 `pending`；不按等待时长自动升级为确定差异。
- 不新增风险接受、忽略、强制闭合或自动修复；不改资金门禁、不新增付款恢复权限。
- 不伪造历史标记；不回填未经证明的 cutover。
- 旧实体新转换独立判定；已有票据不静默删除/关闭；新证据走已批准的失效/复核/审计流程（Q5）。

### 任务映射

- T030（保持未勾选）：Q-cutover 已裁决 B（§4），但判别器未实现前，pre-cutover 请求维持现行 fail-closed 行为；T030 关闭仍依赖 T040。
- T040：按本文实现数据模型＋读路径三路裁决＋旧任务兼容＋失效规则；完整验收清单见 tasks.md T040（同事务提交/回滚/幂等、有预期缺事件、标记事件双缺、无新标记但有充分证据、已证明不适用、跨边界与旧实体新转换、重组/版本/旧任务兼容/原始证据保留、不改资金门禁与无付款恢复权限）。
- T039：CI 路径补齐（`internal/reconciliation/**` 纳入 pg 分类集＋反向覆盖守卫），与本设计无依赖，可并行。
- 本文不构成实现证据；在 T040 验收前不得宣称三路裁决已可用。

## 6. 遗留业务问题（需业务裁决；不得包装为工程实现选择）

1. **祖父证据清单边界**：「已有充分证据」（§2 祖父规则）中哪些现有原子性/覆盖证据构成「充分」——按（转换 × 事件类型）逐项的认定清单、维护者与更新流程，需业务确认，不由实现方自行扩充或缩减。
2. **旧生产者标记缺席的永久 pending 治理**：未写标记的旧生产者产生的义务，其 `pending` 的看板/告警路由、复核节奏与是否存在经批准的关闭路径，需业务给出运营口径；本轮不设自动升级、自动关闭。
3. **期望义务目录的变更权**：新增/调整（转换 × 事件类型）义务属业务/契约变更，其 owner 与变更渠道需明确；实现方只能按已批准目录落地。
